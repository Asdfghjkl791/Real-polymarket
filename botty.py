#!/usr/bin/env python3
"""
Order Book Inspector - find out WHY get_price returns 100¢
Checks the full order book to see what asks/bids actually exist.
"""
import os
import sys
import requests
from datetime import datetime, timezone

try:
    from py_clob_client_v2 import (
        ClobClient, OrderArgs, PartialCreateOrderOptions,
        BalanceAllowanceParams, AssetType, OrderType
    )
except ImportError as e:
    print(f"❌ Import failed: {e}")
    sys.exit(1)

POLY_PRIVATE_KEY = os.environ.get("POLY_PRIVATE_KEY", "").strip()
POLY_FUNDER = os.environ.get("POLY_FUNDER", "").strip()

print("=" * 60)
print("ORDER BOOK INSPECTOR")
print("=" * 60)

# Connect
temp = ClobClient(
    host="https://clob.polymarket.com",
    chain_id=137,
    key=POLY_PRIVATE_KEY,
    signature_type=3,
    funder=POLY_FUNDER,
)
creds = temp.create_or_derive_api_key()
client = ClobClient(
    host="https://clob.polymarket.com",
    chain_id=137,
    key=POLY_PRIVATE_KEY,
    creds=creds,
    signature_type=3,
    funder=POLY_FUNDER,
)
print("Connected\n")

# Loop through current markets - check what asks REALLY exist
now = datetime.now(timezone.utc)
for asset_short in ["btc", "eth", "sol", "doge", "bnb"]:
    for tf in [5, 15]:
        slot = (now.minute // tf) * tf
        open_time = now.replace(minute=slot, second=0, microsecond=0)
        window_ts = int(open_time.timestamp())
        slug = f"{asset_short}-updown-{tf}m-{window_ts}"

        try:
            url = f"https://gamma-api.polymarket.com/events?slug={slug}"
            res = requests.get(url, timeout=10)
            data = res.json()
            if not data:
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
        except Exception as e:
            print(f"  Lookup failed for {slug}: {e}")
            continue

        print(f"\n--- {asset_short.upper()} {tf}m ---")
        for dirn, token_id in [("UP", up_token), ("DOWN", down_token)]:
            # 1. What does get_price say?
            try:
                price_resp = client.get_price(token_id, side="SELL")
                if isinstance(price_resp, dict):
                    gp_ask = float(price_resp.get("price", 0))
                else:
                    gp_ask = float(price_resp)
            except Exception as e:
                gp_ask = None
                print(f"  {dirn} get_price ERROR: {e}")

            # 2. What does the order book look like?
            try:
                book = client.get_order_book(token_id)
                if isinstance(book, dict):
                    asks = book.get("asks", [])
                    bids = book.get("bids", [])
                else:
                    asks = getattr(book, "asks", [])
                    bids = getattr(book, "bids", [])
                ask_count = len(asks)
                bid_count = len(bids)

                # Show top 3 asks (cheapest first - asks are sorted DESC usually)
                # Need to find the lowest ask = best ask
                ask_prices = []
                for a in asks:
                    if isinstance(a, dict):
                        ask_prices.append((float(a.get("price", 0)), float(a.get("size", 0))))
                    else:
                        ask_prices.append((float(getattr(a, "price", 0)), float(getattr(a, "size", 0))))

                if ask_prices:
                    ask_prices.sort()  # cheapest first
                    best_ask = ask_prices[0][0] * 100
                    top_asks_str = ", ".join([f"{p*100:.1f}¢×{s:.1f}" for p, s in ask_prices[:3]])
                else:
                    best_ask = None
                    top_asks_str = "(empty)"

                gp_str = f"{gp_ask*100:.1f}¢" if gp_ask is not None else "ERROR"
                book_str = f"{best_ask:.1f}¢" if best_ask is not None else "no asks"

                # Flag if get_price disagrees with the book
                flag = ""
                if gp_ask is not None and best_ask is not None:
                    if abs(gp_ask*100 - best_ask) > 1:
                        flag = " ⚠️ MISMATCH"

                print(f"  {dirn}: get_price={gp_str} | book_best_ask={book_str} | asks={ask_count} bids={bid_count}{flag}")
                print(f"    top 3 asks: {top_asks_str}")
            except Exception as e:
                print(f"  {dirn} order book ERROR: {e}")

print("\n" + "=" * 60)
print("DONE")
print("=" * 60)
