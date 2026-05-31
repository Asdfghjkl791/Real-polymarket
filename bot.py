#!/usr/bin/env python3
"""
Diagnostic script - tests if the bot CAN place trades on the new account.
Run this once to find out exactly what's blocking trades.

Tests:
  1. Polymarket connection
  2. Balance check
  3. Market lookup
  4. Order book reading
  5. Test order placement (TINY $1 trade, ONLY if ask >= 90¢)
"""
import os
import sys
import time
import requests
from datetime import datetime, timezone

try:
    from py_clob_client_v2 import (
        ClobClient, OrderArgs, PartialCreateOrderOptions,
        BalanceAllowanceParams, AssetType, OrderType
    )
    from py_clob_client_v2.order_builder.constants import BUY
except ImportError as e:
    print(f"❌ Import failed: {e}")
    sys.exit(1)

POLY_PRIVATE_KEY = os.environ.get("POLY_PRIVATE_KEY", "").strip()
POLY_FUNDER = os.environ.get("POLY_FUNDER", "").strip()

if not POLY_PRIVATE_KEY or not POLY_FUNDER:
    print("❌ Missing POLY_PRIVATE_KEY or POLY_FUNDER")
    sys.exit(1)

MIN_ENTRY_CENTS = 90.0  # Only test-trade if ask is >= this

print("=" * 60)
print("POLYMARKET BOT DIAGNOSTIC")
print("=" * 60)
print(f"Funder: {POLY_FUNDER}")
print(f"Min entry: {MIN_ENTRY_CENTS}¢")
print()

# Step 1: Connect
print("[1/5] Connecting to Polymarket...")
try:
    temp = ClobClient(
        host="https://clob.polymarket.com",
        chain_id=137,
        key=POLY_PRIVATE_KEY,
        signature_type=3,
        funder=POLY_FUNDER,
    )
    creds = temp.create_or_derive_api_key()
    print(f"  ✅ API credentials obtained")
    print(f"  Key: {creds.api_key[:8]}...")
except Exception as e:
    print(f"  ❌ Failed: {e}")
    sys.exit(1)

# Build authenticated client
client = ClobClient(
    host="https://clob.polymarket.com",
    chain_id=137,
    key=POLY_PRIVATE_KEY,
    creds=creds,
    signature_type=3,
    funder=POLY_FUNDER,
)
print()

# Step 2: Balance
print("[2/5] Checking balance...")
try:
    params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
    bal_resp = client.get_balance_allowance(params)
    if isinstance(bal_resp, dict):
        bal_raw = bal_resp.get("balance", 0)
    else:
        bal_raw = getattr(bal_resp, "balance", 0)
    balance = float(bal_raw) / 1_000_000
    print(f"  ✅ Balance: ${balance:.2f}")
    if balance < 1.0:
        print(f"  ⚠️ WARNING: Balance below $1.00, can't test trade (min order $1)")
except Exception as e:
    print(f"  ❌ Failed: {e}")
    sys.exit(1)
print()

# Step 3: Find an active market - try multiple to find one with 90¢+ ask
print("[3/5] Finding a current market with ask >= 90¢...")
print("       (trying BTC, ETH, SOL, DOGE, BNB on 5m and 15m)")

found_market = None
now = datetime.now(timezone.utc)

# Build candidate list: try each asset and timeframe
candidates = []
for asset_short in ["btc", "eth", "sol", "doge", "bnb"]:
    for tf in [5, 15]:
        slot = (now.minute // tf) * tf
        open_time = now.replace(minute=slot, second=0, microsecond=0)
        window_ts = int(open_time.timestamp())
        slug = f"{asset_short}-updown-{tf}m-{window_ts}"
        candidates.append((asset_short, tf, slug))

# Loop through candidates and find one with 90¢+ ask
for asset_short, tf, slug in candidates:
    try:
        url = f"https://gamma-api.polymarket.com/events?slug={slug}"
        res = requests.get(url, timeout=10)
        data = res.json()
        if not data or len(data) == 0:
            continue
        event = data[0]
        markets = event.get("markets", [])
        if not markets:
            continue
        market = markets[0]
        token_ids = market.get("clobTokenIds")
        if isinstance(token_ids, str):
            import json
            token_ids = json.loads(token_ids)
        up_token = token_ids[0]
        down_token = token_ids[1]

        # Check ask price for both UP and DOWN, find one >= 90¢
        for dirn, token_id in [("UP", up_token), ("DOWN", down_token)]:
            try:
                price_resp = client.get_price(token_id, side="SELL")
                if isinstance(price_resp, dict):
                    ask_price = float(price_resp.get("price", 0))
                else:
                    ask_price = float(price_resp)
                ask_cents = ask_price * 100
                if ask_cents >= MIN_ENTRY_CENTS and ask_cents <= 99.9:
                    found_market = {
                        "slug": slug,
                        "asset": asset_short.upper(),
                        "tf": tf,
                        "direction": dirn,
                        "token_id": token_id,
                        "up_token": up_token,
                        "down_token": down_token,
                        "ask_price": ask_price,
                        "ask_cents": ask_cents,
                    }
                    break
            except Exception:
                continue
        if found_market:
            break
    except Exception:
        continue

if not found_market:
    print(f"  ⚠️ No market currently has an ask >= {MIN_ENTRY_CENTS}¢")
    print(f"  This is normal - high-confidence prices only appear when crypto has moved.")
    print(f"  Try running again in a few minutes.")
    sys.exit(0)

print(f"  ✅ Found market: {found_market['slug']}")
print(f"  Asset: {found_market['asset']} {found_market['tf']}m {found_market['direction']}")
print(f"  Best ask: {found_market['ask_cents']:.1f}¢")
print()

# Step 4: Read order book for the selected token
print("[4/5] Reading order book...")
try:
    book = client.get_order_book(found_market["token_id"])
    if isinstance(book, dict):
        asks = book.get("asks", [])
        bids = book.get("bids", [])
    else:
        asks = getattr(book, "asks", [])
        bids = getattr(book, "bids", [])
    print(f"  ✅ Order book read")
    print(f"  Asks count: {len(asks)}")
    print(f"  Bids count: {len(bids)}")
except Exception as e:
    print(f"  ⚠️ Order book read failed (not fatal): {e}")
    asks = []
print()

# Step 5: Try a tiny test order
if balance < 1.0:
    print("[5/5] SKIPPING test order (balance below $1)")
    sys.exit(0)

print(f"[5/5] Attempting $1.00 MARKET BUY order on {found_market['asset']} {found_market['tf']}m {found_market['direction']}...")
test_amount = 1.00
print(f"  Ask price: {found_market['ask_cents']:.1f}¢")
print(f"  Amount: ${test_amount} USDC")

try:
    from py_clob_client_v2 import MarketOrderArgs
    order_args = MarketOrderArgs(
        token_id=found_market["token_id"],
        amount=test_amount,
        side=BUY,
        order_type=OrderType.FAK,  # FAK to match the main bot
    )
    resp = client.create_and_post_market_order(
        order_args=order_args,
        options=PartialCreateOrderOptions(tick_size="0.01", neg_risk=False),
        order_type=OrderType.FAK,
    )
    print(f"  Response: {resp}")
    if isinstance(resp, dict):
        success = resp.get("success", False) or resp.get("status") == "matched"
        order_id = resp.get("orderID") or resp.get("orderId", "?")
    else:
        success = getattr(resp, "success", False)
        order_id = getattr(resp, "orderID", "?")
    if success:
        print(f"  ✅ ORDER PLACED SUCCESSFULLY")
        print(f"  Order ID: {order_id}")
        print()
        print(f"  🎉 Bot CAN place trades. No geoblock active.")
    else:
        print(f"  ❌ Order rejected by Polymarket")
        print(f"  Response details: {resp}")
except Exception as e:
    print(f"  ❌ Order failed: {e}")
    err_str = str(e).lower()
    if "403" in err_str or "geoblock" in err_str or "restricted" in err_str:
        print()
        print(f"  🚫 GEO-BLOCK DETECTED")
        print(f"  Polymarket is blocking the server IP from placing trades.")
        print(f"  Need to change region or use different VPS.")
    elif "signer" in err_str or "api key" in err_str:
        print()
        print(f"  🔑 API KEY MISMATCH")
        print(f"  Account API credentials issue - try deleting/regenerating.")
    elif "insufficient" in err_str or "balance" in err_str:
        print()
        print(f"  💰 INSUFFICIENT BALANCE")
        print(f"  Account doesn't have enough USDC.")

print()
print("=" * 60)
print("DIAGNOSIS COMPLETE")
print("=" * 60)
