#!/usr/bin/env python3
"""
Diagnostic script - tests if the bot CAN place trades on the new account.
Run this once to find out exactly what's blocking trades.

Tests:
  1. Polymarket connection
  2. Balance check
  3. Market lookup
  4. Order book reading
  5. Test order placement (TINY $0.10 trade)
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

print("=" * 60)
print("POLYMARKET BOT DIAGNOSTIC")
print("=" * 60)
print(f"Funder: {POLY_FUNDER}")
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

# Step 3: Find an active market
print("[3/5] Finding a current BTC 5m market...")
try:
    now = datetime.now(timezone.utc)
    # Find current 5min window
    slot = (now.minute // 5) * 5
    open_time = now.replace(minute=slot, second=0, microsecond=0)
    window_ts = int(open_time.timestamp())
    slug = f"btc-updown-5m-{window_ts}"

    url = f"https://gamma-api.polymarket.com/events?slug={slug}"
    res = requests.get(url, timeout=10)
    data = res.json()
    if not data or len(data) == 0:
        print(f"  ❌ Market not found: {slug}")
        sys.exit(1)

    event = data[0]
    markets = event.get("markets", [])
    if not markets:
        print(f"  ❌ No markets in event")
        sys.exit(1)

    market = markets[0]
    token_ids = market.get("clobTokenIds")
    if isinstance(token_ids, str):
        import json
        token_ids = json.loads(token_ids)
    up_token = token_ids[0]
    down_token = token_ids[1]
    print(f"  ✅ Found market: {slug}")
    print(f"  UP token: {up_token[:10]}...")
    print(f"  DOWN token: {down_token[:10]}...")
except Exception as e:
    print(f"  ❌ Failed: {e}")
    sys.exit(1)
print()

# Step 4: Read order book
print("[4/5] Reading order book...")
try:
    book = client.get_order_book(up_token)
    if isinstance(book, dict):
        asks = book.get("asks", [])
        bids = book.get("bids", [])
    else:
        asks = getattr(book, "asks", [])
        bids = getattr(book, "bids", [])
    print(f"  ✅ Order book read")
    print(f"  Asks count: {len(asks)}")
    print(f"  Bids count: {len(bids)}")
    if asks:
        first_ask = asks[0]
        if isinstance(first_ask, dict):
            ask_price = float(first_ask.get("price", 0))
            ask_size = float(first_ask.get("size", 0))
        else:
            ask_price = float(getattr(first_ask, "price", 0))
            ask_size = float(getattr(first_ask, "size", 0))
        print(f"  Best ask: {ask_price*100:.1f}¢ (size: {ask_size})")
    if not asks:
        print(f"  ⚠️ No asks available - can't buy")
except Exception as e:
    print(f"  ❌ Failed: {e}")
    sys.exit(1)
print()

# Step 5: Try a tiny test order
if balance < 1.0 or not asks:
    print("[5/5] SKIPPING test order (balance below $1 or no asks)")
    sys.exit(0)

print("[5/5] Attempting TINY $1.00 test order...")
print(f"  Asset: BTC 5m UP")
import math
# Polymarket: cost max 2 decimals, shares max 4 decimals, minimum $1 order
test_size = 1.00
ask_price = round(ask_price, 2)
shares = math.floor((test_size / ask_price) * 100) / 100
if shares * ask_price > test_size:
    shares -= 0.01
shares = round(shares, 2)
print(f"  Price: {ask_price*100:.1f}¢")
print(f"  Size: ${test_size}")
print(f"  Shares: {shares}")

try:
    order_args = OrderArgs(
        token_id=up_token,
        price=ask_price,
        size=shares,
        side=BUY,
    )
    resp = client.create_and_post_order(
        order_args,
        options=PartialCreateOrderOptions(tick_size="0.01", neg_risk=False),
        order_type=OrderType.GTC,  # GTC has more flexible precision than FOK
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
        print(f"  🎉 Bot CAN place trades on this account!")
    else:
        print(f"  ❌ Order rejected by Polymarket")
        print(f"  Response details: {resp}")
except Exception as e:
    print(f"  ❌ Order failed: {e}")
    err_str = str(e).lower()
    if "403" in err_str or "geoblock" in err_str or "restricted" in err_str:
        print()
        print(f"  🚫 GEO-BLOCK DETECTED")
        print(f"  Polymarket is blocking Railway's IP from placing trades.")
        print(f"  Need to change Railway region or use different VPS.")
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
