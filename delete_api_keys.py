#!/usr/bin/env python3
"""
One-time script to delete all API keys on a Polymarket account.
Run this once to clear stale/conflicting API keys.
After running, Polymarket UI manual trades should work again.

Usage:
  Set POLY_PRIVATE_KEY and POLY_FUNDER env vars, then run:
  python delete_api_keys.py
"""
import os
from py_clob_client_v2 import ClobClient

POLY_PRIVATE_KEY = os.environ.get("POLY_PRIVATE_KEY", "").strip()
POLY_FUNDER      = os.environ.get("POLY_FUNDER", "").strip()

if not POLY_PRIVATE_KEY or not POLY_FUNDER:
    print("ERROR: Missing POLY_PRIVATE_KEY or POLY_FUNDER env vars")
    exit(1)

host = "https://clob.polymarket.com"
chain_id = 137

print("Connecting to Polymarket...")
print(f"Funder: {POLY_FUNDER}")

# Create client and derive existing API key
client = ClobClient(
    host=host,
    chain_id=chain_id,
    key=POLY_PRIVATE_KEY,
    signature_type=3,
    funder=POLY_FUNDER,
)

try:
    print("\nDeriving existing API credentials...")
    creds = client.derive_api_key()
    print(f"Got credentials: {creds}")

    # Recreate client with creds
    auth_client = ClobClient(
        host=host,
        chain_id=chain_id,
        key=POLY_PRIVATE_KEY,
        creds=creds,
        signature_type=3,
        funder=POLY_FUNDER,
    )

    print("\nDeleting API key...")
    result = auth_client.delete_api_key()
    print(f"Delete result: {result}")
    print("\n✅ API key deleted. Try manual trade on Polymarket UI again.")
    print("If you have more API keys, run this script again to delete the next one.")

except Exception as e:
    print(f"\n❌ Error: {e}")
    print("\nThis might mean:")
    print("  - No API key exists to delete")
    print("  - Already deleted")
    print("  - Account not provisioned yet (deposit USDC first)")
