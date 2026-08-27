#!/usr/bin/env python3
# LIVE LONGSHOT — buys 1-5c deep-underdog side early in each window (REAL money)
# Ladder: sells LADDER_SELL_FRAC of remaining REAL shares each time the bid
# doubles from the last rung. Remainder rides to settlement.
#
# FIX (2026-08-24): sells were computed off a CALCULATED share estimate
# (stake / ask), not the shares actually received from the buy fill. A market
# buy can fill for slightly fewer shares than the math predicts, so the ladder
# was trying to sell MORE than was actually held -> repeated 400 "not enough
# balance" errors, same rung retried forever, only one rung ever completing.
# Fix: shares_total is now set from the buy response when available, and every
# sell is capped at a tracked real balance, decremented only on confirmed fill.
import os, time, json, sqlite3, logging, threading, requests
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
try:
    import websocket
    WEBSOCKET_AVAILABLE = True
except ImportError:
    WEBSOCKET_AVAILABLE = False

# ── LIVE TRADING (off by default; set LIVE=true + keys to arm) ───────────────
LIVE            = os.environ.get("LIVE", "false").lower() == "true"
POLY_PRIVATE_KEY = os.environ.get("POLY_PRIVATE_KEY", "")
POLY_FUNDER      = os.environ.get("POLY_FUNDER", "")
LIVE_STAKE       = float(os.environ.get("LIVE_STAKE", "5"))
BANKROLL_STOP    = float(os.environ.get("BANKROLL_STOP", "30"))
MAX_OPEN         = int(os.environ.get("MAX_OPEN", "7"))
# Safety margin on every ladder sell: sell slightly LESS than the tracked
# balance so small rounding differences from the exchange never cause a
# "not enough balance" 400 again. 1.0 = no margin; 0.98 = sell 2% less.
SELL_SAFETY_FRAC = float(os.environ.get("SELL_SAFETY_FRAC", "0.98"))
_clob = None
_live_realized = 0.0
try:
    from py_clob_client_v2 import (ClobClient, OrderArgs, MarketOrderArgs,
                                   PartialCreateOrderOptions, OrderType)
    from py_clob_client_v2.order_builder.constants import BUY, SELL
    CLOB_SDK = True
except Exception:
    CLOB_SDK = False


def _clob_init():
    global _clob
    if not (LIVE and CLOB_SDK and POLY_PRIVATE_KEY and POLY_FUNDER):
        return False
    try:
        t = ClobClient(host="https://clob.polymarket.com", chain_id=137,
                       key=POLY_PRIVATE_KEY, signature_type=3, funder=POLY_FUNDER)
        creds = t.create_or_derive_api_key()
        _clob = ClobClient(host="https://clob.polymarket.com", chain_id=137,
                           key=POLY_PRIVATE_KEY, creds=creds, signature_type=3,
                           funder=POLY_FUNDER)
        return True
    except Exception as e:
        log.error(f"[CLOB] init failed: {e}")
        return False


def live_buy(token_id, usdc, est_shares=None):
    """FAK market buy for $usdc. Returns (filled: bool, real_shares: float|None).
    real_shares comes from the fill response so the ladder tracks what was
    ACTUALLY bought, not a price-implied estimate. takingAmount is tried first
    (on a BUY, "taking" is what you receive = shares). Whatever parses is
    sanity-banded against the estimate: a value far outside 30%-120% of the
    estimate is almost certainly a mis-parsed field (e.g. dollars, not shares)
    and is DISCARDED rather than trusted — falling back to the estimate is
    safer than silently tracking 5 "shares" when 100 were bought."""
    if not _clob:
        return False, None
    try:
        a = MarketOrderArgs(token_id=token_id, amount=usdc, side=BUY,
                            order_type=OrderType.FAK)
        r = _clob.create_and_post_market_order(order_args=a,
            options=PartialCreateOrderOptions(tick_size="0.01", neg_risk=False),
            order_type=OrderType.FAK)
        ok = isinstance(r, dict) and (r.get("success") or r.get("status") == "matched")
        real_shares = None
        if ok and isinstance(r, dict):
            for k in ("takingAmount", "filledSize", "size", "matchedAmount",
                      "makingAmount"):
                if r.get(k):
                    try:
                        val = float(r[k])
                    except (TypeError, ValueError):
                        continue
                    if val > 1_000_000:      # raw base units -> shares
                        val = val / 1_000_000
                    if est_shares and not (0.3 * est_shares <= val <= 1.2 * est_shares):
                        log.warning(f"[BUY] field {k}={val:.2f} implausible vs "
                                    f"estimate {est_shares:.2f} — ignoring")
                        continue
                    real_shares = val
                    break
        return ok, real_shares
    except Exception as e:
        log.error(f"[BUY] {e}")
        return False, None


def live_sell_market(token_id, shares):
    """FAK market sell of `shares` at the current bid. True only if filled —
    rungs only advance on CONFIRMED fills, so accounting stays honest."""
    if not _clob:
        return False
    try:
        a = MarketOrderArgs(token_id=token_id, amount=shares, side=SELL,
                            order_type=OrderType.FAK)
        r = _clob.create_and_post_market_order(order_args=a,
            options=PartialCreateOrderOptions(tick_size="0.01", neg_risk=False),
            order_type=OrderType.FAK)
        return isinstance(r, dict) and (r.get("success") or r.get("status") == "matched")
    except Exception as e:
        log.error(f"[SELL] {e}")
        return False


TELEGRAM_TOKEN   = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
PAPER_STAKE      = float(os.environ.get("PAPER_STAKE", "5"))
TAKER_MAX_ASK_CENTS = float(os.environ.get("TAKER_MAX_ASK_CENTS", "5"))
TAKER_MIN_ASK_CENTS = float(os.environ.get("TAKER_MIN_ASK_CENTS", "1"))
DB_PATH          = os.environ.get("DB_PATH", "paper_longshot.db")
SEND_EACH        = os.environ.get("SEND_EACH", "true").lower() == "true"
SETTLE_POLL_SECS    = float(os.environ.get("SETTLE_POLL_SECS", "15"))
SETTLE_TIMEOUT_SECS = float(os.environ.get("SETTLE_TIMEOUT_SECS", "1800"))
MAX_STACK           = int(os.environ.get("MAX_STACK", "1"))
TFS              = [int(x) for x in os.environ.get("TIMEFRAMES", "5,15,60").split(",")]
ENTRY_FIRST_SECS     = float(os.environ.get("ENTRY_FIRST_SECS", "60"))
ENTRY_FIRST_SECS_15M = float(os.environ.get("ENTRY_FIRST_SECS_15M", "180"))
ENTRY_FIRST_SECS_60M = float(os.environ.get("ENTRY_FIRST_SECS_60M", "900"))
ENTRY_FIRST_SECS_240M = float(os.environ.get("ENTRY_FIRST_SECS_240M", "3600"))
OPEN_CAPTURE_GRACE   = float(os.environ.get("OPEN_CAPTURE_GRACE", "3"))

LADDER_ENABLED   = os.environ.get("LADDER_ENABLED", "true").lower() == "true"
LADDER_MULT      = float(os.environ.get("LADDER_MULT", "2.0"))
LADDER_SELL_FRAC = float(os.environ.get("LADDER_SELL_FRAC", "0.5"))
LADDER_TFS       = set(int(x) for x in
                       os.environ.get("LADDER_TFS", "15,60,240").split(","))
LADDER_POLL_SECS = float(os.environ.get("LADDER_POLL_SECS", "3"))
# Minimum share sweep: Polymarket's practical minimum order size. If a rung's
# half-slice would be smaller than this, sell everything remaining instead.
MIN_SELL_SHARES = float(os.environ.get("MIN_SELL_SHARES", "5"))

ASSET_LIST = ["BTC", "ETH", "SOL", "DOGE", "BNB", "XRP", "HYPE"]
ASSET_EMOJI = {"BTC": "🟠", "ETH": "🔷", "SOL": "🟣", "DOGE": "🟡",
               "BNB": "🟨", "XRP": "⚪", "HYPE": "🟢"}
ASSET_FULLNAME = {"BTC":"bitcoin","ETH":"ethereum","SOL":"solana",
                  "DOGE":"dogecoin","BNB":"bnb","XRP":"xrp","HYPE":"hype"}
CLOB_BASE = "https://clob.polymarket.com"
GAMMA_BASE = "https://gamma-api.polymarket.com"
ET = ZoneInfo("America/New_York")
BINANCE_WS = ("wss://data-stream.binance.vision/stream?streams=" +
              "/".join(f"{s}usdt@bookTicker" for s in
                       ["btc", "eth", "sol", "doge", "bnb", "xrp"]))
BINANCE_SYM_TO_ASSET = {f"{s.upper()}USDT": s.upper()
                        for s in ["btc", "eth", "sol", "doge", "bnb", "xrp"]}

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("live-longshot")

prices_ref = {}
ref_last = {}


def binance_ref_worker():
    while True:
        ws = None
        try:
            ws = websocket.create_connection(BINANCE_WS, timeout=10)
            ws.settimeout(30)
            log.info("[REF] Binance.vision reference feed connected")
            while True:
                msg = ws.recv()
                if not msg:
                    continue
                d = json.loads(msg).get("data", {})
                a = BINANCE_SYM_TO_ASSET.get(d.get("s"))
                if a:
                    b, k = float(d.get("b", 0)), float(d.get("a", 0))
                    if b > 0 and k > 0:
                        prices_ref[a] = (b + k) / 2.0
                        ref_last[a] = time.time()
        except Exception as e:
            log.warning(f"[REF] error: {e} — reconnecting")
        finally:
            try:
                ws and ws.close()
            except Exception:
                pass
        time.sleep(3)


def window_times(tf):
    now = time.time()
    if tf == 60:
        now_et = datetime.now(timezone.utc).astimezone(ET)
        start_et = now_et.replace(minute=0, second=0, microsecond=0)
        o = int(start_et.timestamp())
        return o, o + 3600, o + 3600 - now
    if tf == 240:
        now_et = datetime.now(timezone.utc).astimezone(ET)
        block_hour = (now_et.hour // 4) * 4
        start_et = now_et.replace(hour=block_hour, minute=0, second=0, microsecond=0)
        o = int(start_et.timestamp())
        return o, o + 14400, o + 14400 - now
    L = tf * 60
    o = int(now // L) * L
    return o, o + L, o + L - now


def build_slug(asset, tf, open_ts):
    if tf == 240:
        return f"{asset.lower()}-updown-4h-{open_ts}"
    if tf == 60:
        dt_et = datetime.fromtimestamp(open_ts, tz=ET)
        month = dt_et.strftime("%B").lower()
        day = dt_et.day
        year = dt_et.year
        hour12 = dt_et.strftime("%I").lstrip("0") or "12"
        ampm = dt_et.strftime("%p").lower()
        return f"{ASSET_FULLNAME[asset]}-up-or-down-{month}-{day}-{year}-{hour12}{ampm}-et"
    return f"{asset.lower()}-updown-{tf}m-{open_ts}"


_market_cache = {}

def resolve_tokens(asset, tf, open_ts):
    key = (asset, tf, open_ts)
    if key in _market_cache:
        return _market_cache[key]
    slug = build_slug(asset, tf, open_ts)
    try:
        r = requests.get(f"{GAMMA_BASE}/events", params={"slug": slug}, timeout=8)
        arr = r.json()
        ev = arr[0] if isinstance(arr, list) and arr else arr
        markets = ev.get("markets", []) if isinstance(ev, dict) else []
        if markets:
            toks = json.loads(markets[0].get("clobTokenIds", "[]"))
            if len(toks) == 2:
                _market_cache[key] = (toks[0], toks[1])
                return _market_cache[key]
    except Exception as e:
        log.debug(f"[RESOLVE] {slug}: {e}")
    _market_cache[key] = None
    return None


def best_ask_cents(token_id):
    try:
        r = requests.get(f"{CLOB_BASE}/book", params={"token_id": token_id}, timeout=6)
        b = r.json()
        prices = [float(a["price"]) for a in b.get("asks", [])
                  if float(a.get("size", 0)) > 0]
        return min(prices) * 100.0 if prices else None
    except Exception:
        return None


def best_bid_cents(token_id):
    try:
        r = requests.get(f"{CLOB_BASE}/book", params={"token_id": token_id}, timeout=6)
        b = r.json()
        prices = [float(x["price"]) for x in b.get("bids", [])
                  if float(x.get("size", 0)) > 0]
        return max(prices) * 100.0 if prices else None
    except Exception:
        return None


def fetch_polymarket_outcome(asset, tf, open_ts):
    slug = build_slug(asset, tf, open_ts)
    try:
        r = requests.get(f"{GAMMA_BASE}/events", params={"slug": slug}, timeout=10)
        data = r.json()
        if not data or not isinstance(data, list):
            return None
        markets = data[0].get("markets", [])
        if not markets:
            return None
        op = markets[0].get("outcomePrices")
        if isinstance(op, str):
            try:
                op = json.loads(op)
            except Exception:
                pass
        if not op or len(op) < 2:
            return None
        up_p, down_p = float(op[0]), float(op[1])
        if up_p >= 0.99:
            return "UP"
        if down_p >= 0.99:
            return "DOWN"
        return None
    except Exception as e:
        log.warning(f"[OUTCOME] {slug}: {e}")
        return None


def entry_window_secs(tf):
    if tf == 240:
        return ENTRY_FIRST_SECS_240M
    if tf == 60:
        return ENTRY_FIRST_SECS_60M
    if tf == 15:
        return ENTRY_FIRST_SECS_15M
    return ENTRY_FIRST_SECS


def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""CREATE TABLE IF NOT EXISTS paper (
        id INTEGER PRIMARY KEY AUTOINCREMENT, created TEXT, asset TEXT, tf INTEGER,
        direction TEXT, open_ts INTEGER, close_ts INTEGER, secs_left REAL,
        move_pct REAL, ask_cents REAL, open_price REAL, settle_price REAL,
        result TEXT, pnl REAL, ladder_sold REAL DEFAULT 0, ladder_proceeds REAL DEFAULT 0,
        hold_pnl REAL, real_shares REAL)""")
    conn.execute("UPDATE paper SET result='VOID' WHERE result='PENDING'")
    try:
        conn.execute("ALTER TABLE paper ADD COLUMN real_shares REAL")
    except Exception:
        pass
    conn.commit()
    conn.close()


def db_insert(asset, tf, direction, open_ts, close_ts, secs_left, move, ask, op):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""INSERT INTO paper (created,asset,tf,direction,open_ts,close_ts,
                 secs_left,move_pct,ask_cents,open_price,result)
                 VALUES (?,?,?,?,?,?,?,?,?,?, 'PENDING')""",
              (datetime.now(timezone.utc).isoformat(), asset, tf, direction,
               open_ts, close_ts, secs_left, move, ask, op))
    rid = c.lastrowid
    conn.commit()
    conn.close()
    return rid


def db_set_real_shares(rid, val):
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("UPDATE paper SET real_shares=? WHERE id=?", (val, rid))
        conn.commit()
        conn.close()
    except Exception:
        pass


def db_resolve(rid, settle_price, result, pnl):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("UPDATE paper SET settle_price=?, result=?, pnl=? WHERE id=?",
                 (settle_price, result, pnl, rid))
    conn.commit()
    conn.close()


def db_resolve2(rid, settle_price, result, pnl, ladder_sold, ladder_proceeds, hold_pnl):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""UPDATE paper SET settle_price=?, result=?, pnl=?,
                    ladder_sold=?, ladder_proceeds=?, hold_pnl=? WHERE id=?""",
                 (settle_price, result, pnl, ladder_sold, ladder_proceeds,
                  hold_pnl, rid))
    conn.commit()
    conn.close()


def ladder_monitor():
    """Watches each open position's bid; each time it reaches the next rung,
    sells LADDER_SELL_FRAC of remaining REAL shares (never the calculated
    estimate). In LIVE mode a rung only advances on a CONFIRMED fill, and the
    sell amount is capped by SELL_SAFETY_FRAC against the tracked balance so a
    tiny exchange-side rounding difference can't trigger a 400 again."""
    if not LADDER_ENABLED:
        return
    while True:
        try:
            time.sleep(LADDER_POLL_SECS)
            now = time.time()
            with pending_lock:
                items = list(pending)
            for s in items:
                if s["tf"] not in LADDER_TFS:
                    continue
                if now >= s["close_ts"]:
                    continue
                if s.get("shares_left", 0) < 1:
                    continue
                bid = best_bid_cents(s["token"])
                if bid is None:
                    continue
                if bid > s.get("peak_bid", 0.0):
                    s["peak_bid"] = bid
                sold_any = False
                while bid >= s["next_rung"] and s["shares_left"] >= 1:
                    sell_shares = s["shares_left"] * LADDER_SELL_FRAC
                    if sell_shares < MIN_SELL_SHARES:
                        sell_shares = s["shares_left"]
                    if LIVE:
                        # never try to sell more than the tracked real balance,
                        # and shave a safety margin off to survive rounding
                        safe_amt = round(min(sell_shares, s["shares_left"])
                                         * SELL_SAFETY_FRAC, 2)
                        if safe_amt < 1:
                            break
                        if not live_sell_market(s["token"], safe_amt):
                            log.info(f"[LADDER] live sell not filled "
                                     f"{s['asset']} @{bid:.0f}¢ amt={safe_amt} "
                                     f"(tracked left={s['shares_left']:.2f}) "
                                     f"— retry next poll")
                            break  # do NOT advance the rung; retry while bid holds
                        sell_shares = safe_amt
                    proceeds = sell_shares * (bid / 100.0)
                    s["ladder_proceeds"] += proceeds
                    s["ladder_sold"] += sell_shares
                    s["shares_left"] -= sell_shares
                    s["next_rung"] *= LADDER_MULT
                    sold_any = True
                    label = "4h" if s["tf"] == 240 else "1h" if s["tf"] == 60 else f"{s['tf']}m"
                    tg(f"🪜 <b>{ASSET_EMOJI.get(s['asset'],'')}{s['asset']} {label}</b>"
                       f" — sold {sell_shares:.0f} sh @ {bid:.0f}¢ → +${proceeds:.2f}\n"
                       f"banked ${s['ladder_proceeds']:.2f} · "
                       f"{s['shares_left']:.0f} riding · next rung {s['next_rung']:.0f}¢")
                    log.info(f"[LADDER] {s['asset']} {s['tf']} sold {sell_shares:.1f} "
                             f"@ {bid:.0f}¢ banked ${s['ladder_proceeds']:.2f} "
                             f"left {s['shares_left']:.1f}")
                # minimum-share sweep: try to sell small remainders, but
                # dust below the exchange minimum will REJECT — so cap at 3
                # attempts, then let it ride to settlement (the scorer values
                # riding shares correctly, so giving up costs nothing). Sub-1-
                # share dust is written off immediately: never worth an order.
                if s["shares_left"] > 0 and s["shares_left"] < MIN_SELL_SHARES and bid:
                    if s["shares_left"] < 1:
                        s["shares_left"] = 0.0   # write off pennies of dust
                        continue
                    if s.get("sweep_tries", 0) >= 3:
                        continue                 # stop trying; ride to settle
                    s["sweep_tries"] = s.get("sweep_tries", 0) + 1
                    amt = round(s["shares_left"] * (SELL_SAFETY_FRAC if LIVE else 1.0), 2)
                    filled = live_sell_market(s["token"], amt) if LIVE else True
                    if filled and amt > 0:
                        proceeds = amt * (bid / 100.0)
                        s["ladder_proceeds"] += proceeds
                        s["ladder_sold"] += amt
                        s["shares_left"] -= amt
                        tg(f"🪜 <b>{ASSET_EMOJI.get(s['asset'],'')}{s['asset']}</b>"
                           f" — swept {amt:.1f} sh (dust) @ {bid:.0f}¢ → +${proceeds:.2f}\n"
                           f"banked ${s['ladder_proceeds']:.2f} · "
                           f"{s['shares_left']:.0f} riding")
        except Exception as e:
            log.error(f"[LADDER] {e}")


def db_scoreboard():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT result, pnl, ask_cents, tf FROM paper WHERE result IN ('WIN','LOSS')")
    rows = c.fetchall()
    conn.close()
    wins = sum(1 for r in rows if r[0] == "WIN")
    pnl = sum(r[1] or 0 for r in rows)
    avg = (sum(r[2] or 0 for r in rows) / len(rows)) if rows else 0
    wr = (wins / len(rows) * 100) if rows else None
    by_tf = {}
    for r in rows:
        d = by_tf.setdefault(r[3], {"n": 0, "w": 0, "pnl": 0.0})
        d["n"] += 1
        d["w"] += 1 if r[0] == "WIN" else 0
        d["pnl"] += r[1] or 0
    return {"n": len(rows), "wins": wins, "wr": wr, "pnl": pnl,
            "avg_ask": avg, "by_tf": by_tf}


def money(x):
    """+$62.50 / −$5.00 — sign before the dollar, always two decimals."""
    return f"{'+' if x >= 0 else '−'}${abs(x):.2f}"


def tg(msg):
    try:
        r = requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                          json={"chat_id": TELEGRAM_CHAT_ID, "text": msg,
                                "parse_mode": "HTML"}, timeout=8)
        body = {}
        try:
            body = r.json()
        except Exception:
            pass
        if getattr(r, "status_code", 200) != 200 or not body.get("ok", False):
            log.error(f"[TG] REJECTED {getattr(r,'status_code','?')}: {str(body)[:150]}")
            return
        log.info(f"[TG] {msg[:80]}")
    except Exception as e:
        log.error(f"TG error: {e}")


_upd = None

def handle_commands():
    global _upd
    try:
        p = {"timeout": 1}
        if _upd:
            p["offset"] = _upd
        for u in requests.get(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
                              params=p, timeout=5).json().get("result", []):
            _upd = u["update_id"] + 1
            t = u.get("message", {}).get("text", "").strip().lower()
            if str(u.get("message", {}).get("chat", {}).get("id")) != str(TELEGRAM_CHAT_ID):
                continue
            if t == "/stats":
                sb = db_scoreboard()
                mode = "LIVE" if (LIVE and _clob) else "PAPER"
                if sb["n"] == 0:
                    tg(f"📊 <b>LONGSHOT · {mode}</b>\nno settled trades yet")
                    continue
                tf_bits = []
                for tf in sorted(sb["by_tf"]):
                    d = sb["by_tf"][tf]
                    lbl = "4h" if tf == 240 else "1h" if tf == 60 else f"{tf}m"
                    sign = "+" if d["pnl"] >= 0 else "−"
                    tf_bits.append(f"{lbl} {sign}${abs(d['pnl']):.0f}")
                tg(f"📊 <b>LONGSHOT · {mode}</b>\n"
                   f"{sb['n']} trades · {sb['wr']:.1f}% win\n"
                   f"{' · '.join(tf_bits)}\n"
                   f"━━━━━━━━━━\n"
                   f"P&L <b>{money(sb['pnl'])}</b>")
            elif t == "/status":
                mode = "LIVE" if (LIVE and _clob) else "PAPER"
                with pending_lock:
                    snap = [dict(asset=p["asset"], tf=p["tf"],
                                 direction=p["direction"], ask=p["ask"],
                                 banked=p.get("ladder_proceeds", 0.0),
                                 left=p.get("shares_left", 0.0),
                                 rung=p.get("next_rung", 0.0),
                                 peak=p.get("peak_bid", 0.0))
                            for p in pending]
                head = (f"📊 <b>LONGSHOT · {mode} status</b>\n"
                        f"{len(snap)} open / max {MAX_OPEN}")
                if LIVE:
                    bleft = max(0.0, BANKROLL_STOP + _live_realized)
                    head += (f" · bankroll left ${bleft:.2f}/${BANKROLL_STOP:g}\n"
                             f"realized {money(_live_realized)}")
                lines = []
                for p in snap:
                    lbl = "4h" if p["tf"] == 240 else "1h" if p["tf"] == 60 else f"{p['tf']}m"
                    ar = "↑" if p["direction"] == "UP" else "↓"
                    st = (f"banked ${p['banked']:.2f}" if p["banked"] > 0
                          else "no rungs yet")
                    pk2 = f" · peak {p['peak']:.0f}¢" if p['peak'] else ""
                    lines.append(f"{ASSET_EMOJI.get(p['asset'],'')}{p['asset']} "
                                 f"{lbl} {ar} @{p['ask']:.0f}¢\n"
                                 f"  {st} · {p['left']:.0f} riding · "
                                 f"next {p['rung']:.0f}¢{pk2}")
                tg(head + ("\n\n" + "\n".join(lines) if lines else
                           "\n\nno open positions"))
            elif t == "/balance":
                if not (LIVE and _clob):
                    tg("📄 paper mode — no real balance")
                    continue
                bal = None
                try:
                    from py_clob_client_v2.clob_types import (
                        BalanceAllowanceParams, AssetType)
                    params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
                    bal = _clob.get_balance_allowance(params=params)
                except Exception as e1:
                    try:
                        bal = _clob.get_balance_allowance(
                            params={"asset_type": "COLLATERAL"})
                    except Exception as e2:
                        tg(f"⚠️ balance fetch failed: {e1}")
                        continue
                try:
                    raw = (float(bal.get("balance", 0))
                           if isinstance(bal, dict) else float(bal))
                    usdc = raw / 1_000_000 if raw > 1000 else raw
                    tg(f"💰 <b>${usdc:.2f} USDC</b>")
                except Exception:
                    tg(f"💰 balance (raw): {bal}")
            elif t == "/help":
                tg("📖 <b>commands</b>\n"
                   "/stats — scoreboard\n"
                   "/status — open positions\n"
                   "/balance — wallet USDC\n"
                   "/help — this list")
    except Exception:
        pass


open_windows = {}
pending = []
pending_lock = threading.Lock()
fired_count = {}
fired_last = {}


def probe():
    if 60 not in TFS:
        return
    open_ts, close_ts, secs_left = window_times(60)
    found = []
    for a in ASSET_LIST:
        toks = resolve_tokens(a, 60, open_ts)
        if toks:
            found.append(a)
        _market_cache.pop((a, 60, open_ts), None)
    if found:
        tg(f"🔎 <b>Hourly probe</b>: resolved {len(found)}/7 "
           f"({', '.join(found)})\nslug: <code>{build_slug('BTC',60,open_ts)}</code>")
    else:
        tg(f"⚠️ <b>Hourly probe FAILED</b>: 0/7 resolved\n"
           f"tried <code>{build_slug('BTC',60,open_ts)}</code> — 1h won't fire")
    log.info(f"[PROBE] hourly found {found}")


def engine():
    while True:
        try:
            time.sleep(0.5)
            for tf in TFS:
                open_ts, close_ts, secs_left = window_times(tf)
                first_secs = entry_window_secs(tf)
                for asset in ASSET_LIST:
                    ref = prices_ref.get(asset)
                    if ref is None:
                        continue
                    wkey = (asset, tf, open_ts)
                    if wkey not in open_windows:
                        elapsed = (tf * 60) - secs_left
                        if elapsed <= OPEN_CAPTURE_GRACE or tf in (60, 240):
                            open_windows[wkey] = ref
                        else:
                            open_windows[wkey] = None
                        continue
                    op = open_windows[wkey]
                    if op is None:
                        continue
                    move = (ref - op) / op * 100.0
                    if fired_count.get(wkey, 0) >= MAX_STACK:
                        continue
                    elapsed = (tf * 60) - secs_left
                    if elapsed > first_secs:
                        continue
                    toks = resolve_tokens(asset, tf, open_ts)
                    if not toks:
                        continue
                    up_ask = best_ask_cents(toks[0])
                    down_ask = best_ask_cents(toks[1])
                    cand = []
                    if up_ask is not None and TAKER_MIN_ASK_CENTS <= up_ask <= TAKER_MAX_ASK_CENTS:
                        cand.append(("UP", up_ask))
                    if down_ask is not None and TAKER_MIN_ASK_CENTS <= down_ask <= TAKER_MAX_ASK_CENTS:
                        cand.append(("DOWN", down_ask))
                    if not cand:
                        continue
                    direction, ask = min(cand, key=lambda x: x[1])
                    stake = LIVE_STAKE if LIVE else PAPER_STAKE
                    real_shares = None
                    if LIVE:
                        if _live_realized <= -BANKROLL_STOP:
                            continue
                        with pending_lock:
                            if len(pending) >= MAX_OPEN:
                                continue
                        tok_buy = toks[0] if direction == "UP" else toks[1]
                        filled, real_shares = live_buy(
                            tok_buy, stake, est_shares=stake / (ask / 100.0))
                        if not filled:
                            log.warning(f"[LIVE] longshot buy failed {asset} {tf}")
                            continue
                        _lb = "4h" if tf == 240 else "1h" if tf == 60 else f"{tf}m"
                        _ar = "↑" if direction == "UP" else "↓"
                        _sh = real_shares if real_shares else stake / (ask / 100.0)
                        _sht = (f"{_sh:.0f} sh confirmed" if real_shares
                                else f"~{_sh:.0f} sh (est)")
                        tg(f"🟢 <b>{ASSET_EMOJI.get(asset,'')}{asset} {_lb} {_ar}</b>"
                           f" — ${stake:g} @ {ask:.0f}¢\n"
                           f"{_sht} · win pays +${_sh - stake:.0f}")
                    fired_last[wkey] = time.time()
                    rid = db_insert(asset, tf, direction, open_ts, close_ts,
                                    secs_left, move, ask, op)
                    if real_shares:
                        db_set_real_shares(rid, round(real_shares, 4))
                    fired_count[wkey] = fired_count.get(wkey, 0) + 1
                    with pending_lock:
                        # FIX: prefer the REAL filled shares from the buy
                        # response; fall back to the price-implied estimate
                        # only if the exchange didn't report a fill size.
                        shares_total = (real_shares if real_shares
                                       else stake / (ask / 100.0))
                        pending.append({"rid": rid, "asset": asset, "tf": tf,
                                        "direction": direction, "open_ts": open_ts,
                                        "close_ts": close_ts, "open_price": op,
                                        "ask": ask,
                                        "token": toks[0] if direction == "UP" else toks[1],
                                        "shares_total": shares_total,
                                        "shares_left": shares_total,
                                        "next_rung": ask * LADDER_MULT,
                                        "ladder_proceeds": 0.0,
                                        "ladder_sold": 0.0,
                                        "real_shares": real_shares})
                    if SEND_EACH and not LIVE:
                        arrow = "↑" if direction == "UP" else "↓"
                        shares = stake / (ask / 100.0)
                        label = "4h" if tf == 240 else "1h" if tf == 60 else f"{tf}m"
                        tg(f"📄 <b>{ASSET_EMOJI.get(asset,'')}{asset} {label} {arrow}</b>"
                           f" — ${stake:g} @ {ask:.0f}¢\n"
                           f"~{shares:.0f} sh · win pays +${shares - stake:.0f}")
                    log.info(f"[LONGSHOT] {asset} {tf} {direction} ask {ask:.1f}¢ "
                             f"elapsed {elapsed:.0f}s real_shares={real_shares}")
        except Exception as e:
            log.error(f"[ENGINE] {e}")


def scorer():
    while True:
        try:
            time.sleep(0.5)
            now = time.time()
            with pending_lock:
                items = list(pending)
            for s in items:
                if now < s["close_ts"] + 2:
                    continue
                if now - s.get("last_chk", 0) < SETTLE_POLL_SECS:
                    continue
                s["last_chk"] = now
                outcome = fetch_polymarket_outcome(s["asset"], s["tf"], s["open_ts"])
                graded_by = "settlement"
                if outcome is None:
                    if now <= s["close_ts"] + SETTLE_TIMEOUT_SECS:
                        continue
                    settle_px = prices_ref.get(s["asset"])
                    op = s["open_price"]
                    if settle_px is None or op is None or abs((settle_px-op)/op) < 1e-6:
                        db_resolve(s["rid"], None, "VOID", 0)
                        with pending_lock:
                            s in pending and pending.remove(s)
                        continue
                    outcome = "UP" if settle_px > op else "DOWN"
                    graded_by = "feed-fallback"
                won = (s["direction"] == outcome)
                stake0 = LIVE_STAKE if LIVE else PAPER_STAKE
                shares_total = s.get("shares_total", stake0 / (s["ask"] / 100.0))
                hold_pnl = (shares_total * 1.0 - stake0) if won else -stake0
                proceeds = s.get("ladder_proceeds", 0.0)
                shares_left = s.get("shares_left", shares_total)
                remaining_settle = (shares_left * 1.0) if won else 0.0
                ladder_pnl = proceeds + remaining_settle - stake0
                pnl = round(ladder_pnl, 4)
                result = "WIN" if won else "LOSS"
                if LIVE:
                    global _live_realized
                    _live_realized += pnl
                db_resolve2(s["rid"], None, result, pnl, s.get("ladder_sold", 0.0),
                            round(proceeds, 4), round(hold_pnl, 4))
                with pending_lock:
                    s in pending and pending.remove(s)
                sb = db_scoreboard()
                tag = "" if graded_by == "settlement" else " ⚠️ feed-graded"
                label = "4h" if s["tf"] == 240 else "1h" if s["tf"] == 60 else f"{s['tf']}m"
                em = ASSET_EMOJI.get(s['asset'], '')
                foot = f"━ {sb['n']} trades · {money(sb['pnl'])} total"
                pk = s.get("peak_bid")
                pk_seg = f" · peak {pk:.0f}¢" if pk else ""
                if won:
                    detail = (f"${proceeds:.2f} banked + {shares_left:.0f} sh paid out"
                              if proceeds > 0 else f"{shares_left:.0f} sh paid out")
                    tg(f"✅ <b>{em}{s['asset']} {label} WIN {money(pnl)}</b>{tag}\n"
                       f"{detail}{pk_seg}\n{foot}")
                elif pnl > 0:
                    tg(f"💰 <b>{em}{s['asset']} {label}</b> — settled against us, "
                       f"ladder banked it{tag}\n"
                       f"net {money(pnl)} · (holding = {money(hold_pnl)}){pk_seg}\n{foot}")
                elif s.get("ladder_sold", 0) > 0:
                    tg(f"❌ <b>{em}{s['asset']} {label}</b> — ladder softened it{tag}\n"
                       f"banked ${proceeds:.2f} · net {money(pnl)} · "
                       f"(holding = {money(hold_pnl)}){pk_seg}\n{foot}")
                else:
                    need = s.get("next_rung", 0)
                    why = (f"peak {pk:.0f}¢ · needed {need:.0f}¢\n"
                           if pk and need else "")
                    tg(f"❌ <b>{em}{s['asset']} {label}</b> — no rungs hit · "
                       f"{money(pnl)}{tag}\n{why}{foot}")
        except Exception as e:
            log.error(f"[SCORER] {e}")


def main():
    if not WEBSOCKET_AVAILABLE:
        log.error("websocket-client not installed")
        return
    init_db()
    threading.Thread(target=binance_ref_worker, daemon=True).start()
    labels = ", ".join("4h" if t==240 else "1h" if t == 60 else f"{t}m" for t in TFS)
    live_ok = _clob_init() if LIVE else False
    if LIVE and not live_ok:
        tg("🔴 LIVE=true but CLOB client failed (keys/SDK). Running PAPER only.")
    if LIVE and live_ok:
        tg(f"🟢 <b>LIVE LONGSHOT armed</b> — REAL money (share-tracking fix)\n"
           f"ladder: sell {LADDER_SELL_FRAC:.0%} each {LADDER_MULT:g}x rung "
           f"(safety {SELL_SAFETY_FRAC:.0%}) · stake ${LIVE_STAKE:g} · "
           f"bankroll stop ${BANKROLL_STOP:g} · max open {MAX_OPEN}\n"
           f"tf={labels} · band {TAKER_MIN_ASK_CENTS:.0f}-{TAKER_MAX_ASK_CENTS:.0f}¢\n/stats")
    else:
        tg(f"🎰 <b>PAPER LONGSHOT live</b> — no money\n"
           f"buys 1-5¢ side · 5m: first {ENTRY_FIRST_SECS:.0f}s · "
           f"15m: first {ENTRY_FIRST_SECS_15M:.0f}s · 1h: first {ENTRY_FIRST_SECS_60M:.0f}s · "
           f"4h: first {ENTRY_FIRST_SECS_240M:.0f}s\n"
           f"tf={labels} · stake ${PAPER_STAKE:g}\n/stats")
    time.sleep(3)
    probe()
    threading.Thread(target=engine, daemon=True).start()
    threading.Thread(target=scorer, daemon=True).start()
    threading.Thread(target=ladder_monitor, daemon=True).start()
    while True:
        try:
            handle_commands()
        except Exception as e:
            log.error(f"main: {e}")
        time.sleep(1)


if __name__ == "__main__":
    main()
