#!/usr/bin/env python3
# PAPER LONGSHOT v2 — buys the 1-5c deep-underdog side early in each window
# (no money) and records EVERYTHING so we can see what actually works.
#
# TRADING RULE (same idea as v1)
#   - In the first part of each window (5m: 60s, 15m: 180s, 1h: 15min,
#     4h: 1h — all adjustable) it watches both sides of every coin.
#   - If a side's best ask is MIN_CENTS..MAX_CENTS (default 1-5c) it paper-buys
#     $STAKE of the cheapest one — once per market.
#   - Fill walks the REAL ask book (VWAP); the real Polymarket taker fee is
#     charged:  fee = shares * 0.07 * price * (1 - price).
#   - Each position is tracked two ways at once:
#       HOLD   — keep everything to settlement
#       LADDER — sell half each time the bid doubles (2x, 4x, 8x ...), walking
#                the real bid book, sell fees included; rest rides to settle.
#
# WHAT'S NEW IN v2 (the data)
#   - SHADOW SCANS: every SCAN_SECS (5m:20s · 15m:30s · 1h:60s · 4h:180s)
#     for EVERY coin in EVERY window, both books are recorded (no trade).
#     After settlement this answers: at minute X, when the cheap side cost
#     Y cents, how often did it win? Which coins? Which hours?
#   - Every trade records: secs into window, real ask/bid both sides, VWAP,
#     book depth, coin move since window open, peak/low bid after entry.
#
# COMMANDS
#   /stats [all]       trades today (or all): hold vs ladder, before/after fees
#   /timing [tf]       shadow: win rate vs price by minute into the window
#   /prices [tf]       shadow: win rate by price (1c,2c,...20c), entry window
#   /coins [tf]        shadow: by coin (entry-window rule)
#   /hours [tf]        shadow: by time of day (entry-window rule)
#   /bands [tf]        shadow: 1-5c vs 5-10c vs 10-15c vs 15-20c side by side
#   add a band to /timing /coins /hours, e.g.  /timing 15m 15-20
#   /export            trades CSV
#   /exportscans [d]   shadow scans, last d days (default 3), gzipped CSV
#   /exportmkts        settled outcomes per market CSV
#   /status /mute /unmute /help
#
# HONEST LIMITS
#   - Longshots win rarely (~2-5%). You need HUNDREDS of trades per bucket
#     before a win rate means anything. One lucky +$150 hit can fake an edge.
#   - All coins in a window move together — count WINDOWS, not trades.
#   - Fee is ~7% of the stake at 1-5c (the highest % of any price).
#
# ENV (required): TELEGRAM_TOKEN, TELEGRAM_CHAT_ID — use its OWN bot token.
# ENV (optional): STAKE=5, TIMEFRAMES=5,15,60,240, COINS=BTC,...,HYPE,
#   MIN_CENTS=1, MAX_CENTS=5, MAX_STACK=1,
#   ENTRY_FIRST_SECS=60 (5m), ENTRY_FIRST_SECS_15M=180, ENTRY_FIRST_SECS_60M=900,
#   ENTRY_FIRST_SECS_240M=3600,
#   LADDER_ENABLED=true, LADDER_MULT=2.0, LADDER_SELL_FRAC=0.5, LADDER_TFS=15,60,240,
#   SCAN_ENABLED=true, SCAN_SECS_5M=20, SCAN_SECS_15M=30, SCAN_SECS_60M=60,
#   SCAN_SECS_240M=180, SCAN_VWAP_MAX_CENTS=25, BANDS=1-5,5-10,10-15,15-20, SCAN_KEEP_DAYS=30,
#   FEE_RATE=0.07, DB_PATH=paper_longshot_v2.db, STATS_TZ=America/Thunder_Bay,
#   SEND_EACH=false (a message per trade), SETTLE_POLL_SECS=15,
#   SETTLE_TIMEOUT_SECS=1800

import os, time, json, sqlite3, logging, threading, requests, csv, io, gzip
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
try:
    import websocket
    WEBSOCKET_AVAILABLE = True
except ImportError:
    WEBSOCKET_AVAILABLE = False


def env_bool(name, default):
    return os.environ.get(name, default).strip().lower() in ("1", "true", "yes", "on")


TELEGRAM_TOKEN   = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
STAKE            = float(os.environ.get("STAKE", os.environ.get("PAPER_STAKE", "5")))
DB_PATH          = os.environ.get("DB_PATH", "paper_longshot_v2.db")
TFS = [int(x) for x in os.environ.get("TIMEFRAMES", "5,15,60,240").split(",") if x.strip()]
COINS = [c.strip().upper() for c in
         os.environ.get("COINS", "BTC,ETH,SOL,DOGE,BNB,XRP,HYPE").split(",") if c.strip()]
MIN_CENTS = float(os.environ.get("MIN_CENTS", os.environ.get("TAKER_MIN_ASK_CENTS", "1")))
MAX_CENTS = float(os.environ.get("MAX_CENTS", os.environ.get("TAKER_MAX_ASK_CENTS", "5")))
MAX_STACK = int(os.environ.get("MAX_STACK", "1"))
ENTRY_SECS = {
    5:   float(os.environ.get("ENTRY_FIRST_SECS", "60")),
    15:  float(os.environ.get("ENTRY_FIRST_SECS_15M", "180")),
    60:  float(os.environ.get("ENTRY_FIRST_SECS_60M", "900")),
    240: float(os.environ.get("ENTRY_FIRST_SECS_240M", "3600")),
    1440: -1.0,  # daily: scan only, never paper-trades
}
LADDER_ENABLED   = env_bool("LADDER_ENABLED", "true")
LADDER_MULT      = float(os.environ.get("LADDER_MULT", "2.0"))
LADDER_SELL_FRAC = float(os.environ.get("LADDER_SELL_FRAC", "0.5"))
LADDER_TFS = {int(x) for x in os.environ.get("LADDER_TFS", "15,60,240").split(",") if x.strip()}
SCAN_ENABLED = env_bool("SCAN_ENABLED", "true")
SCAN_SECS = {
    5:   float(os.environ.get("SCAN_SECS_5M", "20")),
    15:  float(os.environ.get("SCAN_SECS_15M", "30")),
    60:  float(os.environ.get("SCAN_SECS_60M", "60")),
    240: float(os.environ.get("SCAN_SECS_240M", "180")),
    1440: float(os.environ.get("SCAN_SECS_1D", "300")),
}
SCAN_VWAP_MAX_CENTS = float(os.environ.get("SCAN_VWAP_MAX_CENTS", "25"))
SCAN_KEEP_DAYS = float(os.environ.get("SCAN_KEEP_DAYS", "30"))
# price bands tracked on the side (shadow only — the bot still TRADES MIN..MAX)
BANDS = [tuple(float(v) for v in b.split("-")) for b in
         os.environ.get("BANDS", "1-5,5-10,10-15,15-20").split(",") if "-" in b]
# how often to look for entries / walk ladders, per timeframe
POLL = {5: 2.0, 15: 3.0, 60: 8.0, 240: 15.0, 1440: 30.0}
FEE_RATE = float(os.environ.get("FEE_RATE", "0.07"))
SEND_EACH = env_bool("SEND_EACH", "false")
SETTLE_POLL_SECS    = float(os.environ.get("SETTLE_POLL_SECS", "15"))
SETTLE_TIMEOUT_SECS = float(os.environ.get("SETTLE_TIMEOUT_SECS", "1800"))
STATS_TZ = ZoneInfo(os.environ.get("STATS_TZ", "America/Thunder_Bay"))

ASSET_EMOJI = {"BTC": "🟠", "ETH": "🔷", "SOL": "🟣", "DOGE": "🟡",
               "BNB": "🟨", "XRP": "⚪", "HYPE": "🟢"}
ASSET_FULLNAME = {"BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana",
                  "DOGE": "dogecoin", "BNB": "bnb", "XRP": "xrp", "HYPE": "hype"}
CLOB_BASE = "https://clob.polymarket.com"
GAMMA_BASE = "https://gamma-api.polymarket.com"
ET = ZoneInfo("America/New_York")
BINANCE_WS = ("wss://data-stream.binance.vision/stream?streams=" +
              "/".join(f"{s}usdt@bookTicker" for s in
                       ["btc", "eth", "sol", "doge", "bnb", "xrp"]))
BINANCE_SYM_TO_ASSET = {f"{s.upper()}USDT": s.upper()
                        for s in ["btc", "eth", "sol", "doge", "bnb", "xrp"]}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("paper-longshot-v2")

prices_ref = {}
muted = False
db_lock = threading.Lock()
_pool = ThreadPoolExecutor(max_workers=16)


def money(x):
    return f"{'+' if x >= 0 else chr(0x2212)}${abs(x):.2f}"


def label_for(tf):
    return "1d" if tf == 1440 else "4h" if tf == 240 else "1h" if tf == 60 else f"{tf}m"


def parse_tf(arg):
    a = (arg or "").strip().lower()
    return {"5": 5, "5m": 5, "15": 15, "15m": 15, "60": 60, "1h": 60,
            "240": 240, "4h": 240, "1440": 1440, "1d": 1440, "24h": 1440}.get(a)


def parse_args(args):
    """-> (tf or None, (lo, hi) band or None) from things like ['15m', '10-15']"""
    tf, band = None, None
    for a in args:
        if parse_tf(a):
            tf = parse_tf(a)
        elif "-" in a:
            try:
                lo, hi = (float(v.rstrip("c¢")) for v in a.split("-"))
                band = (lo, hi)
            except ValueError:
                pass
    return tf, band


def band_label(band):
    return f"{band[0]:g}–{band[1]:g}¢"


def fee_for(shares, price_frac):
    return shares * FEE_RATE * price_frac * (1.0 - price_frac)


# ── reference feed (coin move since window open; fallback grading) ───────────
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
        except Exception as e:
            log.warning(f"[REF] error: {e} — reconnecting")
        finally:
            try:
                ws and ws.close()
            except Exception:
                pass
        time.sleep(3)


# ── market plumbing ─────────────────────────────────────────────────────────
def window_times(tf, now=None):
    now = time.time() if now is None else now
    if tf == 1440:
        # daily market: noon ET -> next noon ET (Binance 1m close at noon)
        now_et = datetime.fromtimestamp(now, tz=ET)
        st = now_et.replace(hour=12, minute=0, second=0, microsecond=0)
        if now_et < st:
            st = st - timedelta(days=1)
        en = (st + timedelta(days=1)).replace(hour=12)
        o, c = int(st.timestamp()), int(en.timestamp())
        return o, c, c - now
    if tf == 60:
        now_et = datetime.fromtimestamp(now, tz=ET)
        o = int(now_et.replace(minute=0, second=0, microsecond=0).timestamp())
        return o, o + 3600, o + 3600 - now
    if tf == 240:
        now_et = datetime.fromtimestamp(now, tz=ET)
        bh = (now_et.hour // 4) * 4
        o = int(now_et.replace(hour=bh, minute=0, second=0, microsecond=0).timestamp())
        return o, o + 14400, o + 14400 - now
    L = tf * 60
    o = int(now // L) * L
    return o, o + L, o + L - now


def build_slug(asset, tf, open_ts):
    if tf == 1440:
        d = datetime.fromtimestamp(open_ts, tz=ET) + timedelta(days=1)
        return (f"{ASSET_FULLNAME[asset]}-up-or-down-on-{d.strftime('%B').lower()}-"
                f"{d.day}-{d.year}")
    if tf == 240:
        return f"{asset.lower()}-updown-4h-{open_ts}"
    if tf == 60:
        dt_et = datetime.fromtimestamp(open_ts, tz=ET)
        month = dt_et.strftime("%B").lower()
        hour12 = dt_et.strftime("%I").lstrip("0") or "12"
        ampm = dt_et.strftime("%p").lower()
        return (f"{ASSET_FULLNAME[asset]}-up-or-down-{month}-{dt_et.day}-"
                f"{dt_et.year}-{hour12}{ampm}-et")
    return f"{asset.lower()}-updown-{tf}m-{open_ts}"


_market_cache = {}
_market_miss = {}     # key -> time of last failed lookup (retry after 30s)

def resolve_tokens(asset, tf, open_ts):
    key = (asset, tf, open_ts)
    if _market_cache.get(key):
        return _market_cache[key]
    if time.time() - _market_miss.get(key, 0) < 30:
        return None
    if len(_market_miss) > 5000:
        _market_miss.clear()
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
    _market_miss[key] = time.time()
    return None


def get_book(token_id):
    """-> dict(bid, ask in cents, asks ascending [(px, size)], bids descending)"""
    try:
        r = requests.get(f"{CLOB_BASE}/book", params={"token_id": token_id}, timeout=6)
        b = r.json()
        asks = sorted((float(a["price"]) * 100.0, float(a["size"]))
                      for a in b.get("asks", []) if float(a.get("size", 0)) > 0)
        bids = sorted(((float(x["price"]) * 100.0, float(x["size"]))
                       for x in b.get("bids", []) if float(x.get("size", 0)) > 0),
                      reverse=True)
        if not asks and not bids:
            return None
        return {"ask": asks[0][0] if asks else None, "bid": bids[0][0] if bids else None,
                "asks": asks, "bids": bids}
    except Exception:
        return None


def vwap_fill(asks, dollars):
    """Walk the ask book to spend `dollars` -> (vwap_cents, shares, spent, levels)"""
    spent = shares = 0.0
    levels = 0
    for px, sz in asks:
        if spent >= dollars - 1e-9:
            break
        take = min(px / 100.0 * sz, dollars - spent)
        shares += take / (px / 100.0)
        spent += take
        levels += 1
    if shares <= 0:
        return None
    return spent / shares * 100.0, shares, spent, levels


def full_vwap(asks, dollars):
    """average price (cents) to spend `dollars` now, or None if the book can't fill 99% of it"""
    f = vwap_fill(asks, dollars)
    if not f or f[2] < dollars * 0.99:
        return None
    return round(f[0], 3)


def depth_1c(asks):
    """$ for sale within 1 cent of the best ask"""
    if not asks:
        return None
    top = asks[0][0]
    return round(sum(px / 100.0 * sz for px, sz in asks if px <= top + 1), 1)


def sell_fill(bids, shares):
    """Walk the bid book to sell `shares` -> (proceeds_dollars, shares_sold, fee)"""
    left, proceeds, fee = shares, 0.0, 0.0
    for px, sz in bids:
        if left <= 1e-9:
            break
        q = min(sz, left)
        proceeds += q * px / 100.0
        fee += fee_for(q, px / 100.0)
        left -= q
    return proceeds, shares - left, fee


def fetch_outcome(asset, tf, open_ts):
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


# ── database ────────────────────────────────────────────────────────────────
def db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with db_lock:
        conn = db()
        conn.execute("""CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT, created TEXT, entry_ts REAL,
            asset TEXT, tf INTEGER, direction TEXT, open_ts INTEGER, close_ts INTEGER,
            secs_into REAL, secs_left REAL, ask REAL, bid REAL, opp_ask REAL, opp_bid REAL,
            vwap REAL, shares REAL, stake REAL, fee REAL, levels INTEGER, top_size REAL,
            ask_depth_5c REAL, partial INTEGER, ref_open REAL, ref_entry REAL, move_pct REAL,
            ladder_sold REAL DEFAULT 0, ladder_proceeds REAL DEFAULT 0, ladder_fee REAL DEFAULT 0,
            rungs INTEGER DEFAULT 0, peak_bid_after REAL, peak_secs_left REAL,
            min_bid_after REAL, result TEXT, graded_by TEXT,
            hold_pnl REAL, hold_net REAL, ladder_pnl REAL, ladder_net REAL)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS scans (
            id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, asset TEXT, tf INTEGER,
            open_ts INTEGER, secs_into REAL, up_ask REAL, up_bid REAL,
            dn_ask REAL, dn_bid REAL, cheap TEXT, cheap_ask REAL, cheap_vwap REAL,
            cheap_shares REAL, cheap_depth REAL, move_pct REAL)""")
        for col in ("up_v100", "up_v500", "up_v1000", "dn_v100", "dn_v500", "dn_v1000",
                    "up_d1", "dn_d1"):
            try:
                conn.execute(f"ALTER TABLE scans ADD COLUMN {col} REAL")
            except Exception:
                pass   # column already exists
        conn.execute("CREATE INDEX IF NOT EXISTS scans_mkt ON scans(asset, tf, open_ts)")
        conn.execute("CREATE INDEX IF NOT EXISTS scans_ts ON scans(ts)")
        conn.execute("""CREATE TABLE IF NOT EXISTS mkts (
            asset TEXT, tf INTEGER, open_ts INTEGER, close_ts INTEGER,
            outcome TEXT, graded_by TEXT, PRIMARY KEY (asset, tf, open_ts))""")
        conn.execute("UPDATE trades SET result='VOID' WHERE result='PENDING'")
        conn.commit()
        conn.close()


def db_exec(sql, args=(), many=False):
    with db_lock:
        conn = db()
        c = conn.cursor()
        (c.executemany if many else c.execute)(sql, args)
        rid = c.lastrowid
        conn.commit()
        conn.close()
        return rid


def db_query(sql, args=()):
    with db_lock:
        conn = db()
        rows = conn.execute(sql, args).fetchall()
        conn.close()
        return rows


def db_insert_trade(row):
    keys = list(row.keys())
    return db_exec(f"INSERT INTO trades ({','.join(keys)}) VALUES "
                   f"({','.join('?' * len(keys))})", [row[k] for k in keys])


# ── telegram ────────────────────────────────────────────────────────────────
def tg(msg, force=False):
    if muted and not force:
        return
    try:
        r = requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                          json={"chat_id": TELEGRAM_CHAT_ID, "text": msg[:4000],
                                "parse_mode": "HTML"}, timeout=8)
        if r.status_code != 200:
            log.error(f"[TG] {r.status_code}: {r.text[:150]}")
    except Exception as e:
        log.error(f"TG error: {e}")


def tg_document(filename, data, caption="", mime="text/csv"):
    try:
        if isinstance(data, str):
            data = data.encode("utf-8")
        r = requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument",
                          data={"chat_id": TELEGRAM_CHAT_ID, "caption": caption[:1000]},
                          files={"document": (filename, data, mime)}, timeout=300)
        if r.status_code != 200:
            tg(f"⚠️ upload failed ({r.status_code}) — file may be over 50MB, "
               f"try fewer days", force=True)
    except Exception as e:
        tg(f"⚠️ upload failed: {e}", force=True)


def rows_to_csv(rows):
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(rows[0].keys())
    w.writerows([list(r) for r in rows])
    return buf.getvalue()


# ── reports ─────────────────────────────────────────────────────────────────
def today_start():
    return datetime.now(STATS_TZ).replace(hour=0, minute=0, second=0,
                                          microsecond=0).timestamp()


def stats_text(mode="today"):
    since = 0 if mode == "all" else today_start()
    rows = db_query("SELECT * FROM trades WHERE result IN ('WIN','LOSS') AND entry_ts>=?",
                    (since,))
    head = f"🎰 <b>LONGSHOT v2 · PAPER</b> · {'all time' if mode == 'all' else 'today'}"
    if not rows:
        return f"{head}\nno settled trades yet"
    def block(rs):
        n = len(rs)
        w = sum(r["result"] == "WIN" for r in rs)
        px = sum(r["vwap"] for r in rs) / n
        be = sum(r["vwap"] / 100 * (1 + r["fee"] / r["stake"]) for r in rs) / n * 100
        return (n, w, px, be, sum(r["hold_pnl"] for r in rs), sum(r["hold_net"] for r in rs),
                sum(r["ladder_pnl"] for r in rs), sum(r["ladder_net"] for r in rs),
                len({(r["tf"], r["open_ts"]) for r in rs}))
    n, w, px, be, h, hn, l, ln, win = block(rows)
    fees = sum(r["fee"] + (r["ladder_fee"] or 0) for r in rows)
    lines = [head,
             f"{n} trades · {win} windows · <b>{w} won ({w / n * 100:.1f}%)</b>",
             f"avg paid {px:.1f}¢ → need {be:.1f}% wins to break even",
             f"HOLD   before {money(h)} · <b>after fees {money(hn)}</b>",
             f"LADDER before {money(l)} · <b>after fees {money(ln)}</b>",
             f"fees paid ${fees:.2f}", "━━━━━━━━━━"]
    for tf in sorted({r["tf"] for r in rows}):
        rs = [r for r in rows if r["tf"] == tf]
        n2, w2, px2, be2, h2, hn2, l2, ln2, _ = block(rs)
        lines.append(f"<b>{label_for(tf)}</b> {n2} · {w2} won ({w2 / n2 * 100:.1f}% vs "
                     f"{be2:.1f}% needed) · hold {money(hn2)} · ladder {money(ln2)}")
    best = max(rows, key=lambda r: r["hold_pnl"])
    if best["result"] == "WIN":
        lines.append(f"biggest win: {best['asset']} {label_for(best['tf'])} "
                     f"@{best['vwap']:.1f}¢ → {money(best['hold_net'])}")
    lines.append("(after-fees numbers are the real ones)")
    return "\n".join(lines)


def shadow_rows(tf=None, band=None):
    """Every graded scan where the cheap side was inside the band (default: the
    trading band MIN..MAX). Bands are lo < price <= hi (lo included if <= 1c)."""
    lo, hi = band or (MIN_CENTS, MAX_CENTS)
    lo_q = lo if lo <= max(1.0, MIN_CENTS) and not band else (lo if lo <= 1 else lo + 1e-6)
    q = """SELECT s.asset, s.tf, s.open_ts, s.secs_into, s.ts, s.cheap, s.cheap_ask,
                  s.cheap_vwap, s.cheap_shares, m.outcome
           FROM scans s JOIN mkts m ON m.asset=s.asset AND m.tf=s.tf AND m.open_ts=s.open_ts
           WHERE m.outcome IN ('UP','DOWN') AND s.cheap_ask BETWEEN ? AND ?
             AND s.cheap_vwap IS NOT NULL"""
    args = [lo_q, hi]
    if tf:
        q += " AND s.tf=?"
        args.append(tf)
    q += " ORDER BY s.ts"
    return db_query(q, args)


def graded(r):
    p = r["cheap_vwap"] / 100.0
    sh = r["cheap_shares"]
    stake = sh * p
    fee = fee_for(sh, p)
    won = r["cheap"] == r["outcome"]
    hold = (sh - stake) if won else -stake
    return won, hold, hold - fee, p, fee / stake if stake else 0


def summarize(rs):
    n = len(rs)
    if not n:
        return None
    g = [graded(r) for r in rs]
    w = sum(x[0] for x in g)
    px = sum(x[3] for x in g) / n * 100
    be = sum(x[3] * (1 + x[4]) for x in g) / n * 100   # win rate needed after fees
    return n, w, px, be, sum(x[2] for x in g), len({(r["tf"], r["open_ts"]) for r in rs})


def fmt_line(label, s):
    n, w, px, be, net, win = s
    return (f"{label}: {n} mkts/{win} win · {w} won {w / n * 100:.1f}% vs "
            f"{be:.1f}% needed · {money(net)}")


def first_per_market(rows, key_extra=None):
    seen, out = set(), []
    for r in rows:
        k = (r["asset"], r["tf"], r["open_ts"]) + ((key_extra(r),) if key_extra else ())
        if k not in seen:
            seen.add(k)
            out.append(r)
    return out


def timing_text(tf=None, band=None):
    rows = shadow_rows(tf, band)
    if not rows:
        return "⏱ <b>LONGSHOT TIMING</b>\nno graded scans yet (needs a few windows)"
    lines = [f"⏱ <b>LONGSHOT TIMING</b> (shadow · cheap side "
             f"{band_label(band or (MIN_CENTS, MAX_CENTS))} "
             f"· first look per market per bucket · $5 hold · after fees)"]
    for t in sorted({r["tf"] for r in rows}):
        L = t * 60
        nb = 10 if t != 5 else 5
        size = L / nb
        rs = [r for r in rows if r["tf"] == t]
        lines.append(f"━━ <b>{label_for(t)}</b> (entry rule: first {ENTRY_SECS[t] / 60:g} min)")
        for b in range(nb):
            sub = first_per_market([r for r in rs if b * size <= r["secs_into"] < (b + 1) * size])
            s = summarize(sub)
            if s:
                a0, a1 = b * size / 60, (b + 1) * size / 60
                lines.append(fmt_line(f"{a0:g}–{a1:g}m", s))
    return "\n".join(lines)


def entry_rule_rows(tf=None, band=None):
    rows = [r for r in shadow_rows(tf, band) if r["secs_into"] <= ENTRY_SECS.get(r["tf"], 0)]
    return first_per_market(rows)


def bands_text(tf=None):
    lines = ["📊 <b>LONGSHOT BANDS</b> (shadow · first look per market · $5 hold · "
             "after fees)\n<i>entry</i> = inside the entry window · <i>any</i> = any time"]
    found = False
    tfs = [tf] if tf else TFS
    for t in tfs:
        block = [f"━━ <b>{label_for(t)}</b>"]
        for band in BANDS:
            rows = shadow_rows(t, band)
            ent = summarize(first_per_market(
                [r for r in rows if r["secs_into"] <= ENTRY_SECS.get(t, 0)]))
            anyt = summarize(first_per_market(rows))
            if ent:
                block.append(fmt_line(f"{band_label(band)} entry", ent))
            if anyt:
                block.append(fmt_line(f"{band_label(band)} any", anyt))
        if len(block) > 1:
            found = True
            lines += block
    return "\n".join(lines) if found else "📊 <b>LONGSHOT BANDS</b>\nno graded scans yet"


def prices_text(tf=None):
    q = """SELECT s.asset, s.tf, s.open_ts, s.secs_into, s.ts, s.cheap, s.cheap_ask,
                  s.cheap_vwap, s.cheap_shares, m.outcome
           FROM scans s JOIN mkts m ON m.asset=s.asset AND m.tf=s.tf AND m.open_ts=s.open_ts
           WHERE m.outcome IN ('UP','DOWN') AND s.cheap_ask <= 20
             AND s.cheap_vwap IS NOT NULL""" + (" AND s.tf=?" if tf else "") + " ORDER BY s.ts"
    rows = db_query(q, (tf,) if tf else ())
    rows = [r for r in rows if r["secs_into"] <= ENTRY_SECS.get(r["tf"], 0)]
    if not rows:
        return "💲 <b>LONGSHOT PRICES</b>\nno graded scans yet"
    lines = ["💲 <b>LONGSHOT PRICES</b> (shadow · entry window · first look per market "
             "at each price · after fees)"]
    for t in sorted({r["tf"] for r in rows}):
        rs = [r for r in rows if r["tf"] == t]
        lines.append(f"━━ <b>{label_for(t)}</b>")
        for c in range(1, 21):
            sub = first_per_market([r for r in rs if c - 0.5 < r["cheap_ask"] <= c + 0.5])
            s = summarize(sub)
            if s:
                lines.append(fmt_line(f"{c}¢", s))
    return "\n".join(lines)


def group_text(title, keyfn, order, tf=None, band=None):
    rows = entry_rule_rows(tf, band)
    if not rows:
        return f"{title}\nno graded scans yet"
    lines = [f"{title} (shadow · {band_label(band or (MIN_CENTS, MAX_CENTS))} in entry "
             f"window · after fees)"]
    for t in sorted({r["tf"] for r in rows}):
        rs = [r for r in rows if r["tf"] == t]
        lines.append(f"━━ <b>{label_for(t)}</b>")
        for k in order:
            s = summarize([r for r in rs if keyfn(r) == k])
            if s:
                lines.append(fmt_line(str(k), s))
    return "\n".join(lines)


def hour_block(r):
    h = datetime.fromtimestamp(r["open_ts"], tz=STATS_TZ).hour
    return f"{h // 4 * 4:02d}-{h // 4 * 4 + 4:02d}h"


def coins_text(tf=None, band=None):
    return group_text("🪙 <b>LONGSHOT BY COIN</b>", lambda r: r["asset"], COINS, tf, band)


def hours_text(tf=None, band=None):
    order = [f"{h:02d}-{h + 4:02d}h" for h in range(0, 24, 4)]
    return group_text("🕒 <b>LONGSHOT BY TIME OF DAY</b>", hour_block, order, tf, band)


# ── trading ─────────────────────────────────────────────────────────────────
pending = []
pending_lock = threading.Lock()
fired = {}            # (asset, tf, open_ts) -> count
open_ref = {}         # (tf, open_ts) -> {asset: ref price}
due_mkts = {}         # (asset, tf, open_ts) -> close_ts   (markets awaiting outcome)
due_lock = threading.Lock()


def mark_due(asset, tf, open_ts, close_ts):
    with due_lock:
        due_mkts[(asset, tf, open_ts)] = close_ts


def move_pct(tf, open_ts, asset):
    op = open_ref.get((tf, open_ts), {}).get(asset)
    px = prices_ref.get(asset)
    if op and px:
        return round((px - op) / op * 100.0, 4), op, px
    return None, op, px


def fetch_books(tf, open_ts, assets):
    toks = {a: resolve_tokens(a, tf, open_ts) for a in assets}
    jobs = {a: (_pool.submit(get_book, tk[0]), _pool.submit(get_book, tk[1]))
            for a, tk in toks.items() if tk}
    out = {}
    for a, (fu, fd) in jobs.items():
        up, dn = fu.result(), fd.result()
        if up and dn:
            out[a] = (up, dn, toks[a])
    return out


def try_entries(tf, open_ts, close_ts, secs_left):
    todo = [a for a in COINS if fired.get((a, tf, open_ts), 0) < MAX_STACK]
    if not todo:
        return
    books = fetch_books(tf, open_ts, todo)
    for a, (up, dn, tk) in books.items():
        cand = []
        for d, bk, tok, other in (("UP", up, tk[0], dn), ("DOWN", dn, tk[1], up)):
            if bk["ask"] is not None and MIN_CENTS <= bk["ask"] <= MAX_CENTS:
                cand.append((bk["ask"], d, bk, tok, other))
        if not cand:
            continue
        ask, direction, bk, tok, other = min(cand, key=lambda x: x[0])
        fill = vwap_fill(bk["asks"], STAKE)
        if not fill:
            continue
        vwap, shares, spent, levels = fill
        fee = fee_for(shares, vwap / 100.0)
        fired[(a, tf, open_ts)] = fired.get((a, tf, open_ts), 0) + 1
        mv, op, px = move_pct(tf, open_ts, a)
        now = time.time()
        row = dict(created=datetime.now(timezone.utc).isoformat(), entry_ts=now, asset=a,
                   tf=tf, direction=direction, open_ts=open_ts, close_ts=close_ts,
                   secs_into=round(tf * 60 - secs_left, 1), secs_left=round(secs_left, 1),
                   ask=ask, bid=bk["bid"], opp_ask=other["ask"], opp_bid=other["bid"],
                   vwap=round(vwap, 4), shares=round(shares, 4), stake=round(spent, 4),
                   fee=round(fee, 5), levels=levels, top_size=bk["asks"][0][1],
                   ask_depth_5c=round(sum(s for p, s in bk["asks"] if p <= MAX_CENTS), 1),
                   partial=int(spent < STAKE - 1e-6), ref_open=op, ref_entry=px,
                   move_pct=mv, result="PENDING")
        rid = db_insert_trade(row)
        with pending_lock:
            pending.append(dict(row, rid=rid, token=tok, shares_left=shares,
                                next_rung=vwap * LADDER_MULT, ladder_sold=0.0,
                                ladder_proceeds=0.0, ladder_fee=0.0, rungs=0,
                                peak_bid=bk["bid"] or 0.0, peak_secs_left=secs_left,
                                min_bid=bk["bid"] or 0.0))
        mark_due(a, tf, open_ts, close_ts)
        log.info(f"[ENTRY] {a} {label_for(tf)} {direction} vwap {vwap:.2f}c "
                 f"{shares:.0f}sh fee ${fee:.3f} {tf * 60 - secs_left:.0f}s in")
        if SEND_EACH:
            tg(f"🎰 {ASSET_EMOJI.get(a, '')}{a} {label_for(tf)} "
               f"{'⬆️' if direction == 'UP' else '⬇️'} @{vwap:.1f}¢ · {shares:.0f} sh · "
               f"win +${shares - spent:.0f}")


def walk_positions(tfs_due):
    """Track peak/low bid for every open position; run the paper ladder."""
    now = time.time()
    with pending_lock:
        live = [s for s in pending if s["tf"] in tfs_due and now < s["close_ts"]]
    if not live:
        return
    futs = [(s, _pool.submit(get_book, s["token"])) for s in live]
    for s, f in futs:
        bk = f.result()
        if not bk or bk["bid"] is None:
            continue
        bid = bk["bid"]
        if bid > s["peak_bid"]:
            s["peak_bid"], s["peak_secs_left"] = bid, s["close_ts"] - now
        s["min_bid"] = min(s["min_bid"], bid)
        if not (LADDER_ENABLED and s["tf"] in LADDER_TFS):
            continue
        while bid >= s["next_rung"] and s["shares_left"] >= 1:
            want = s["shares_left"] * LADDER_SELL_FRAC
            proceeds, sold, fee = sell_fill(bk["bids"], want)
            if sold <= 0:
                break
            s["ladder_proceeds"] += proceeds
            s["ladder_sold"] += sold
            s["ladder_fee"] += fee
            s["shares_left"] -= sold
            s["rungs"] += 1
            s["next_rung"] *= LADDER_MULT
            log.info(f"[LADDER] {s['asset']} {label_for(s['tf'])} sold {sold:.0f} @~{bid:.0f}c")


def settle_trade(s, outcome, graded_by):
    won = s["direction"] == outcome
    hold = (s["shares"] - s["stake"]) if won else -s["stake"]
    hold_net = hold - s["fee"]
    lad = s["ladder_proceeds"] + (s["shares_left"] if won else 0.0) - s["stake"]
    lad_net = lad - s["fee"] - s["ladder_fee"]
    db_exec("""UPDATE trades SET result=?, graded_by=?, hold_pnl=?, hold_net=?,
               ladder_pnl=?, ladder_net=?, ladder_sold=?, ladder_proceeds=?, ladder_fee=?,
               rungs=?, peak_bid_after=?, peak_secs_left=?, min_bid_after=? WHERE id=?""",
            ("WIN" if won else "LOSS", graded_by, round(hold, 4), round(hold_net, 4),
             round(lad, 4), round(lad_net, 4), round(s["ladder_sold"], 4),
             round(s["ladder_proceeds"], 4), round(s["ladder_fee"], 5), s["rungs"],
             s["peak_bid"], round(s["peak_secs_left"], 1), s["min_bid"], s["rid"]))
    if won:
        tg(f"✅ <b>LONGSHOT HIT</b> {ASSET_EMOJI.get(s['asset'], '')}{s['asset']} "
           f"{label_for(s['tf'])} {s['direction']} @{s['vwap']:.1f}¢\n"
           f"hold {money(hold_net)} · ladder {money(lad_net)} (after fees)")


# ── shadow scanner ──────────────────────────────────────────────────────────
scan_done = set()     # (tf, open_ts, bucket)


def do_scan(tf, open_ts, close_ts, secs_left):
    books = fetch_books(tf, open_ts, COINS)
    now = time.time()
    rows = []
    for a, (up, dn, _tk) in books.items():
        ua, da = up["ask"], dn["ask"]
        if ua is None and da is None:
            continue
        if da is None or (ua is not None and ua <= da):
            cheap, bk = "UP", up
        else:
            cheap, bk = "DOWN", dn
        vw = sh = depth = None
        if bk["ask"] is not None and bk["ask"] <= SCAN_VWAP_MAX_CENTS:
            fill = vwap_fill(bk["asks"], STAKE)
            if fill:
                vw, sh = round(fill[0], 4), round(fill[1], 4)
            depth = round(sum(s for p, s in bk["asks"] if p <= bk["ask"] + 1), 1)
        mv, _op, _px = move_pct(tf, open_ts, a)
        dep = [full_vwap(up["asks"], 100), full_vwap(up["asks"], 500), full_vwap(up["asks"], 1000),
               full_vwap(dn["asks"], 100), full_vwap(dn["asks"], 500), full_vwap(dn["asks"], 1000),
               depth_1c(up["asks"]), depth_1c(dn["asks"])]
        rows.append((now, a, tf, open_ts, round(now - open_ts, 1), ua, up["bid"],
                     da, dn["bid"], cheap, bk["ask"], vw, sh, depth, mv, *dep))
        mark_due(a, tf, open_ts, close_ts)
    if rows:
        db_exec("""INSERT INTO scans (ts,asset,tf,open_ts,secs_into,up_ask,up_bid,dn_ask,
                   dn_bid,cheap,cheap_ask,cheap_vwap,cheap_shares,cheap_depth,move_pct,
                   up_v100,up_v500,up_v1000,dn_v100,dn_v500,dn_v1000,up_d1,dn_d1)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", rows, many=True)


def scanner():
    last_prune = 0
    while True:
        try:
            time.sleep(1.0)
            for tf in TFS:
                open_ts, close_ts, secs_left = window_times(tf)
                into = time.time() - open_ts
                b = int(into // SCAN_SECS[tf])
                key = (tf, open_ts, b)
                if key in scan_done or secs_left < 2:
                    continue
                scan_done.add(key)
                do_scan(tf, open_ts, close_ts, secs_left)
            if time.time() - last_prune > 3600:
                last_prune = time.time()
                old = time.time() - 26 * 3600
                scan_done.difference_update([k for k in scan_done if k[1] < old])
                for k in [k for k in _market_cache if k[2] < old]:
                    _market_cache.pop(k, None)
                db_exec("DELETE FROM scans WHERE ts < ?",
                        (time.time() - SCAN_KEEP_DAYS * 86400,))
        except Exception as e:
            log.error(f"[SCAN] {e}")


# ── settlement ──────────────────────────────────────────────────────────────
def grader():
    while True:
        try:
            time.sleep(SETTLE_POLL_SECS)
            now = time.time()
            with due_lock:
                due = [(k, c) for k, c in due_mkts.items() if now >= c + 2]
            for (asset, tf, open_ts), close_ts in due:
                outcome = fetch_outcome(asset, tf, open_ts)
                graded_by = "settlement"
                if outcome is None:
                    if now <= close_ts + SETTLE_TIMEOUT_SECS:
                        continue
                    op = open_ref.get((tf, open_ts), {}).get(asset)
                    px = prices_ref.get(asset)
                    if op and px and abs(px - op) / op > 1e-6:
                        outcome, graded_by = ("UP" if px > op else "DOWN"), "feed-fallback"
                    else:
                        outcome, graded_by = "VOID", "none"
                db_exec("INSERT OR REPLACE INTO mkts VALUES (?,?,?,?,?,?)",
                        (asset, tf, open_ts, close_ts, outcome, graded_by))
                with pending_lock:
                    mine = [s for s in pending if (s["asset"], s["tf"], s["open_ts"]) ==
                            (asset, tf, open_ts)]
                    for s in mine:
                        pending.remove(s)
                for s in mine:
                    if outcome == "VOID":
                        db_exec("UPDATE trades SET result='VOID', hold_pnl=0, hold_net=0, "
                                "ladder_pnl=0, ladder_net=0 WHERE id=?", (s["rid"],))
                    else:
                        settle_trade(s, outcome, graded_by)
                with due_lock:
                    due_mkts.pop((asset, tf, open_ts), None)
        except Exception as e:
            log.error(f"[GRADER] {e}")


# ── main loops ──────────────────────────────────────────────────────────────
def trader():
    last = {}
    while True:
        try:
            time.sleep(0.5)
            now = time.time()
            for tf in TFS:
                open_ts, close_ts, secs_left = window_times(tf, now)
                if (tf, open_ts) not in open_ref:
                    # only trust the reference if we saw the window open
                    fresh = now - open_ts <= 5
                    open_ref[(tf, open_ts)] = dict(prices_ref) if fresh else {}
                    old = now - 26 * 3600
                    for k in [k for k in open_ref if k[1] < old]:
                        open_ref.pop(k, None)
                    for k in [k for k in fired if k[2] < old]:
                        fired.pop(k, None)
                if now - last.get(tf, 0) < POLL.get(tf, 5.0):
                    continue
                last[tf] = now
                if tf * 60 - secs_left <= ENTRY_SECS.get(tf, 0):
                    try_entries(tf, open_ts, close_ts, secs_left)
                walk_positions({tf})
        except Exception as e:
            log.error(f"[TRADER] {e}")


_upd = None


# ── daily-market history backfill (/history1d) ──────────────────────────────
BINANCE_REST = os.environ.get("BINANCE_REST", "https://data-api.binance.vision/api/v3/klines")
_hist_running = threading.Event()


def _get_json(url, params, tries=3):
    for i in range(tries):
        try:
            r = requests.get(url, params=params, timeout=15)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                time.sleep(2 + 2 * i)
                continue
            return None
        except Exception:
            time.sleep(1 + i)
    return None


def _price_history(token, o, c):
    """-> (fidelity_used, [(t, price_cents)]) for one outcome token"""
    tries = [dict(startTs=o - 3600, endTs=c + 120, fidelity=5),
             dict(startTs=o - 3600, endTs=c + 120, fidelity=60),
             dict(interval="max", fidelity=60),
             dict(interval="max", fidelity=720)]
    for q in tries:
        j = _get_json(f"{CLOB_BASE}/prices-history", dict(market=token, **q))
        h = (j or {}).get("history") or []
        h = [(int(x["t"]), round(float(x["p"]) * 100, 3)) for x in h
             if "t" in x and "p" in x and o - 3600 <= int(x["t"]) <= c + 120]
        if h:
            return q["fidelity"], h
        time.sleep(0.1)
    return None, []


def _klines(sym, interval, start_ms, end_ms, limit=1000):
    j = _get_json(BINANCE_REST, dict(symbol=sym, interval=interval, startTime=start_ms,
                                     endTime=end_ms, limit=limit))
    return j if isinstance(j, list) else []


def daily_history(days):
    try:
        mrows = ["asset,day,open_ts,close_ts,outcome,up_token,dn_token,volume,"
                 "start_open,start_close,end_open,end_close,fid_up,fid_dn,n_up,n_dn"]
        prows = ["asset,day,side,t,price"]
        krows = ["asset,day,t,open,high,low,close"]
        today = datetime.now(ET).date()
        found = 0
        for back in range(1, days + 1):
            d = today - timedelta(days=back)
            en = datetime(d.year, d.month, d.day, 12, tzinfo=ET)
            st = en - timedelta(days=1)
            o, c = int(st.timestamp()), int(en.timestamp())
            for a in COINS:
                ev = _get_json(f"{GAMMA_BASE}/events", {"slug": build_slug(a, 1440, o)})
                ev = ev[0] if isinstance(ev, list) and ev else ev
                mk = (ev or {}).get("markets") or [] if isinstance(ev, dict) else []
                if not mk:
                    continue
                m = mk[0]
                try:
                    toks = json.loads(m.get("clobTokenIds") or "[]")
                    names = [x.lower() for x in json.loads(m.get("outcomes") or '["Up","Down"]')]
                    op = m.get("outcomePrices")
                    op = json.loads(op) if isinstance(op, str) else op
                except Exception:
                    continue
                if len(toks) != 2:
                    continue
                iu = names.index("up") if "up" in names else 0
                up_t, dn_t = toks[iu], toks[1 - iu]
                outcome = ""
                try:
                    if float(op[iu]) >= 0.99:
                        outcome = "UP"
                    elif float(op[1 - iu]) >= 0.99:
                        outcome = "DOWN"
                except Exception:
                    pass
                fu, hu = _price_history(up_t, o, c)
                fd, hd = _price_history(dn_t, o, c)
                for side, h in (("UP", hu), ("DOWN", hd)):
                    prows += [f"{a},{d},{side},{t},{px}" for t, px in h]
                so = sc = eo = ec = ""
                sym = f"{a}USDT"
                k1 = _klines(sym, "1m", o * 1000, o * 1000 + 59000, 2)
                k2 = _klines(sym, "1m", c * 1000, c * 1000 + 59000, 2)
                if k1:
                    so, sc = k1[0][1], k1[0][4]
                if k2:
                    eo, ec = k2[0][1], k2[0][4]
                k5 = _klines(sym, "5m", o * 1000, c * 1000, 1000)
                krows += [f"{a},{d},{int(k[0]) // 1000},{k[1]},{k[2]},{k[3]},{k[4]}" for k in k5]
                mrows.append(f"{a},{d},{o},{c},{outcome},{up_t},{dn_t},"
                             f"{m.get('volume', '')},{so},{sc},{eo},{ec},{fu or ''},{fd or ''},"
                             f"{len(hu)},{len(hd)}")
                found += 1
                time.sleep(0.15)
            if back % 10 == 0:
                tg(f"⏳ daily history: {back}/{days} days · {found} markets so far", force=True)
        tg_document("daily_mkts.csv", "\n".join(mrows), f"{found} daily markets · {days} days")
        tg_document("daily_prices.csv.gz", gzip.compress("\n".join(prows).encode()),
                    f"{len(prows) - 1} price points", "application/gzip")
        tg_document("daily_klines.csv.gz", gzip.compress("\n".join(krows).encode()),
                    f"{len(krows) - 1} Binance 5m candles", "application/gzip")
    except Exception as e:
        tg(f"⚠️ /history1d failed: {e}", force=True)
    finally:
        _hist_running.clear()


# ── multi-timeframe history backfill (/history 4h|1h|15m|klines [days]) ──────
_hist2_running = threading.Event()
HIST_DIR = os.environ.get("HIST_DIR", os.path.dirname(os.path.abspath(DB_PATH)) or ".")


def _hist_windows(tf, days):
    """all window open times for tf over the last `days` full days (ET), oldest first"""
    now = time.time()
    end_o, _c, _l = window_times(tf, now)          # current (unfinished) window: skip it
    start = now - days * 86400
    out, o = [], end_o
    while True:
        prev, _c2, _l2 = window_times(tf, o - 1)
        if prev < start:
            break
        out.append(prev)
        o = prev
    return out[::-1]


def _one_market(a, tf, o):
    c = window_times(tf, o + 1)[1]
    ev = _get_json(f"{GAMMA_BASE}/events", {"slug": build_slug(a, tf, o)})
    ev = ev[0] if isinstance(ev, list) and ev else ev
    mk = ((ev or {}).get("markets") or []) if isinstance(ev, dict) else []
    if not mk:
        return None
    m = mk[0]
    try:
        toks = json.loads(m.get("clobTokenIds") or "[]")
        names = [x.lower() for x in json.loads(m.get("outcomes") or '["Up","Down"]')]
        op = m.get("outcomePrices")
        op = json.loads(op) if isinstance(op, str) else op
    except Exception:
        return None
    if len(toks) != 2:
        return None
    iu = names.index("up") if "up" in names else 0
    outcome = ""
    try:
        if float(op[iu]) >= 0.99:
            outcome = "UP"
        elif float(op[1 - iu]) >= 0.99:
            outcome = "DOWN"
    except Exception:
        pass
    res = {"mk": None, "px": []}
    fids = []
    for side, tok in (("UP", toks[iu]), ("DOWN", toks[1 - iu])):
        got = []
        for fid in (1, 5):
            j = _get_json(f"{CLOB_BASE}/prices-history",
                          dict(market=tok, startTs=o - 60, endTs=c + 60, fidelity=fid))
            got = [(int(x["t"]), round(float(x["p"]) * 100, 3))
                   for x in ((j or {}).get("history") or [])
                   if "t" in x and "p" in x and o - 60 <= int(x["t"]) <= c + 60]
            if got:
                break
        fids.append(fid if got else "")
        res["px"] += [f"{a},{tf},{o},{side},{t},{px}" for t, px in got]
    res["mk"] = (f"{a},{tf},{o},{c},{outcome},{m.get('volume', '')},"
                 f"{fids[0]},{fids[1]}")
    return res


def history_tf(tf, days):
    try:
        wins = _hist_windows(tf, days)
        if tf == 5:   # 5m: weekday mornings only (8:30-11:30 ET) to keep it small
            def _keep(o):
                d = datetime.fromtimestamp(o, tz=ET)
                return d.weekday() < 5 and 510 <= d.hour * 60 + d.minute < 690
            wins = [o for o in wins if _keep(o)]
        jobs = [(a, tf, o) for o in wins for a in COINS]
        lab = label_for(tf)
        tg(f"⏳ /history {lab}: {len(jobs)} markets to fetch", force=True)
        mpath = os.path.join(HIST_DIR, f"hist_{lab}_mkts.csv")
        ppath = os.path.join(HIST_DIR, f"hist_{lab}_prices.csv.gz")
        n_ok = n_px = 0
        with open(mpath, "w") as fm, gzip.open(ppath, "wt") as fp:
            fm.write("asset,tf,open_ts,close_ts,outcome,volume,fid_up,fid_dn\n")
            fp.write("asset,tf,open_ts,side,t,price\n")
            step = max(1, len(jobs) // 10)
            with ThreadPoolExecutor(max_workers=6) as ex:
                for i, r in enumerate(ex.map(lambda j: _one_market(*j), jobs), 1):
                    if r:
                        fm.write(r["mk"] + "\n")
                        if r["px"]:
                            fp.write("\n".join(r["px"]) + "\n")
                        n_ok += 1
                        n_px += len(r["px"])
                    if i % step == 0:
                        tg(f"⏳ /history {lab}: {i}/{len(jobs)} · {n_ok} found", force=True)
        with open(mpath, "rb") as f:
            tg_document(os.path.basename(mpath), f.read(), f"{n_ok} {lab} markets · {days} days")
        with open(ppath, "rb") as f:
            tg_document(os.path.basename(ppath), f.read(), f"{n_px} price points",
                        "application/gzip")
    except Exception as e:
        tg(f"⚠️ /history {tf} failed: {e}", force=True)
    finally:
        _hist2_running.clear()


def history_klines(days):
    try:
        path = os.path.join(HIST_DIR, "hist_klines_1m.csv.gz")
        end = int(time.time() // 60) * 60
        start = end - days * 86400
        n = 0
        with gzip.open(path, "wt") as f:
            f.write("asset,t,open,high,low,close,volume\n")
            for a in COINS:
                t = start
                while t < end:
                    k = _klines(f"{a}USDT", "1m", t * 1000, min(end, t + 60000) * 1000 - 1, 1000)
                    if k:
                        f.write("\n".join(f"{a},{int(x[0]) // 1000},{x[1]},{x[2]},{x[3]},{x[4]},{x[5]}"
                                          for x in k) + "\n")
                        n += len(k)
                    t += 60000
                    time.sleep(0.05)
        with open(path, "rb") as f:
            tg_document("hist_klines_1m.csv.gz", f.read(), f"{n} Binance 1m candles · {days} days",
                        "application/gzip")
    except Exception as e:
        tg(f"⚠️ /history klines failed: {e}", force=True)
    finally:
        _hist2_running.clear()


# ── past TRADES (who bought how much at what price) — /tradehist ─────────────
DATA_API = "https://data-api.polymarket.com"
_trade_running = threading.Event()


def _market_meta(a, tf, o):
    ev = _get_json(f"{GAMMA_BASE}/events", {"slug": build_slug(a, tf, o)})
    ev = ev[0] if isinstance(ev, list) and ev else ev
    mk = ((ev or {}).get("markets") or []) if isinstance(ev, dict) else []
    if not mk:
        return None
    m = mk[0]
    try:
        toks = json.loads(m.get("clobTokenIds") or "[]")
        names = [x.lower() for x in json.loads(m.get("outcomes") or '["Up","Down"]')]
    except Exception:
        return None
    if len(toks) != 2 or not m.get("conditionId"):
        return None
    iu = names.index("up") if "up" in names else 0
    return m["conditionId"], {toks[iu]: "UP", toks[1 - iu]: "DOWN"}


def _market_trades(cond, tokmap, o, c, max_rows=20000):
    out, off = [], 0
    while off < max_rows:
        j = _get_json(f"{DATA_API}/trades", {"market": cond, "limit": 500, "offset": off,
                                             "takerOnly": "false"})
        if not isinstance(j, list) or not j:
            break
        for t in j:
            try:
                ts = int(t.get("timestamp", 0))
                if ts < o - 3600 or ts > c + 120:
                    continue
                side = tokmap.get(str(t.get("asset")), t.get("outcome", ""))
                out.append((ts, str(side).upper(), str(t.get("side", "")).upper(),
                            round(float(t.get("price", 0)) * 100, 2), round(float(t.get("size", 0)), 2)))
            except Exception:
                continue
        if len(j) < 500:
            break
        off += 500
        time.sleep(0.05)
    return out


def trade_history(tf, days, coins, hours):
    try:
        wins = _hist_windows(tf, days)
        if hours:
            lo, hi = hours
            wins = [o for o in wins if datetime.fromtimestamp(o, tz=ET).weekday() < 5 and
                    lo <= datetime.fromtimestamp(o, tz=ET).hour * 60 + datetime.fromtimestamp(o, tz=ET).minute < hi]
        jobs = [(a, o) for o in wins for a in coins]
        lab = label_for(tf)
        tg(f"⏳ /tradehist {lab}: {len(jobs)} markets ({', '.join(coins)})", force=True)
        path = os.path.join(HIST_DIR, f"trades_{lab}.csv.gz")
        n_m = n_t = 0
        step = max(1, len(jobs) // 10)

        def one(job):
            a, o = job
            meta = _market_meta(a, tf, o)
            if not meta:
                return a, o, []
            c = window_times(tf, o + 1)[1]
            return a, o, _market_trades(meta[0], meta[1], o, c)
        with gzip.open(path, "wt") as f:
            f.write("asset,tf,open_ts,t,outcome,side,price,size\n")
            with ThreadPoolExecutor(max_workers=4) as ex:
                for i, (a, o, rows) in enumerate(ex.map(one, jobs), 1):
                    if rows:
                        n_m += 1
                        n_t += len(rows)
                        f.write("\n".join(f"{a},{tf},{o},{t},{oc},{sd},{px},{sz}"
                                          for t, oc, sd, px, sz in rows) + "\n")
                    if i % step == 0:
                        tg(f"⏳ /tradehist {lab}: {i}/{len(jobs)} · {n_t} trades", force=True)
        with open(path, "rb") as f:
            data = f.read()
        tg_document(os.path.basename(path), data, f"{n_t} trades in {n_m} {lab} markets · {days} days",
                    "application/gzip")
    except Exception as e:
        tg(f"⚠️ /tradehist failed: {e}", force=True)
    finally:
        _trade_running.clear()


def handle_commands():
    global _upd, muted
    try:
        p = {"timeout": 1}
        if _upd:
            p["offset"] = _upd
        for u in requests.get(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
                              params=p, timeout=5).json().get("result", []):
            _upd = u["update_id"] + 1
            if str(u.get("message", {}).get("chat", {}).get("id")) != str(TELEGRAM_CHAT_ID):
                continue
            parts = u.get("message", {}).get("text", "").strip().lower().split()
            if not parts:
                continue
            t = parts[0].split("@")[0]
            try:
                run_command(t, parts[1:])
            except Exception as e:
                tg(f"⚠️ {t} failed: {e}", force=True)
    except Exception:
        pass


def run_command(t, args=()):
    global muted
    if isinstance(args, str):
        args = args.split()
    arg = args[0] if args else ""
    tf, band = parse_args(args)
    if t == "/stats":
        tg(stats_text("all" if arg == "all" else "today"), force=True)
    elif t == "/timing":
        tg(timing_text(tf, band), force=True)
    elif t == "/prices":
        tg(prices_text(tf), force=True)
    elif t == "/coins":
        tg(coins_text(tf, band), force=True)
    elif t == "/hours":
        tg(hours_text(tf, band), force=True)
    elif t == "/bands":
        tg(bands_text(tf), force=True)
    elif t == "/status":
        with pending_lock:
            snap = list(pending)
        if not snap:
            tg("🎰 no open positions", force=True)
        else:
            lines = [f"🎰 <b>{len(snap)} open</b>"]
            for s in snap[:40]:
                lines.append(f"{ASSET_EMOJI.get(s['asset'], '')}{s['asset']} "
                             f"{label_for(s['tf'])} {'↑' if s['direction'] == 'UP' else '↓'} "
                             f"@{s['vwap']:.1f}¢ · peak bid {s['peak_bid']:.0f}¢")
            tg("\n".join(lines), force=True)
    elif t == "/export":
        rows = db_query("SELECT * FROM trades ORDER BY id")
        if rows:
            tg_document("longshot_trades.csv", rows_to_csv(rows), f"{len(rows)} trades")
        else:
            tg("no trades yet", force=True)
    elif t == "/exportscans":
        try:
            days = max(0.1, min(30.0, float(arg))) if arg else 3.0
        except ValueError:
            days = 3.0
        rows = db_query("SELECT * FROM scans WHERE ts >= ? ORDER BY id",
                        (time.time() - days * 86400,))
        if rows:
            data = gzip.compress(rows_to_csv(rows).encode())
            tg_document(f"longshot_scans_{days:g}d.csv.gz", data,
                        f"{len(rows)} scans · {days:g} day(s)", "application/gzip")
        else:
            tg("no scans yet", force=True)
    elif t == "/exportmkts":
        rows = db_query("SELECT * FROM mkts ORDER BY open_ts")
        if rows:
            tg_document("longshot_mkts.csv", rows_to_csv(rows), f"{len(rows)} markets")
        else:
            tg("no settled markets yet", force=True)
    elif t == "/history1d":
        try:
            days = max(1, min(365, int(float(arg)))) if arg else 60
        except ValueError:
            days = 60
        if _hist_running.is_set():
            tg("⏳ already running — wait for the files", force=True)
        else:
            _hist_running.set()
            tg(f"⏳ pulling {days} days of daily (24h) markets — takes a few minutes, "
               f"3 files will arrive", force=True)
            threading.Thread(target=daily_history, args=(days,), daemon=True).start()
    elif t == "/history":
        which = args[0] if args and args[0] in ("klines", "4h", "1h", "15m", "5m") else ""
        nums = [a for a in args[1:] if a.replace(".", "").isdigit()]
        try:
            days = max(1, min(90, int(float(nums[0])))) if nums else 60
        except ValueError:
            days = 60
        if not which:
            tg("use: /history 4h 60 · /history 1h 60 · /history 15m 30 · /history 5m 60 · /history klines 60",
               force=True)
        elif _hist2_running.is_set():
            tg("⏳ a /history run is already going — wait for its files", force=True)
        else:
            _hist2_running.set()
            if which == "klines":
                tg(f"⏳ pulling {days} days of Binance 1m candles…", force=True)
                threading.Thread(target=history_klines, args=(days,), daemon=True).start()
            else:
                tfv = parse_tf(which)
                threading.Thread(target=history_tf, args=(tfv, days), daemon=True).start()
    elif t == "/tradehist":
        presets = {"5m": (5, ["BTC", "ETH", "SOL", "XRP", "DOGE"], (540, 660)),    # weekday 9-11am
                   "15m": (15, ["BTC", "ETH", "SOL", "XRP", "DOGE"], (540, 660)),
                   "1h": (60, ["BTC", "ETH"], None),                               # all hours
                   "1hall": (60, ["BTC", "ETH", "SOL", "XRP", "DOGE"], None)}
        which = args[0] if args and args[0] in presets else ""
        try:
            days = max(1, min(60, int(float(args[1])))) if len(args) > 1 else 14
        except ValueError:
            days = 14
        if not which:
            tg("use: /tradehist 5m 14 · /tradehist 15m 14 · /tradehist 1h 14 · /tradehist 1hall 14", force=True)
        elif _trade_running.is_set():
            tg("⏳ a /tradehist run is already going — wait for its file", force=True)
        else:
            _trade_running.set()
            tfv, coins, hrs = presets[which]
            threading.Thread(target=trade_history, args=(tfv, days, coins, hrs), daemon=True).start()
    elif t == "/mm":
        if not MM_ENABLED:
            tg("🏦 paper market maker is OFF — set MM_ENABLED=true in Railway to start it", force=True)
        else:
            tg(mm_text("all" if arg == "all" else "today"), force=True)
    elif t == "/mmexport":
        rows = db_query("SELECT * FROM mm_mkts ORDER BY open_ts") if MM_ENABLED else []
        fills = db_query("SELECT * FROM mm_fills ORDER BY id") if MM_ENABLED else []
        if rows:
            tg_document("mm_markets.csv", rows_to_csv(rows), f"{len(rows)} settled paper-MM markets")
        if fills:
            tg_document("mm_fills.csv.gz", gzip.compress(rows_to_csv(fills).encode()),
                        f"{len(fills)} paper-MM fills", "application/gzip")
        if not rows and not fills:
            tg("no paper-MM data yet", force=True)
    elif t == "/mute":
        muted = True
        tg("🔇 muted — still trading and recording. /unmute to turn messages back on",
           force=True)
    elif t == "/unmute":
        muted = False
        tg("🔔 unmuted")
    elif t == "/help":
        tg("📖 <b>LONGSHOT v2 commands</b>\n"
           "/stats — today: hold vs ladder, before/after fees\n/stats all — all time\n"
           "/timing [5m|15m|1h|4h] — win rate by minute into the window\n"
           "/prices [tf] — win rate at 1¢, 2¢ … 20¢\n"
           "/coins [tf] — by coin · /hours [tf] — by time of day\n"
           "/bands [tf] — 1-5¢ vs 5-10¢ vs 10-15¢ vs 15-20¢ side by side\n"
           "add a band to timing/coins/hours: <code>/timing 15m 15-20</code>\n"
           "/status — open positions\n/export — trades CSV\n"
           "/exportscans [days] — raw shadow scans (gz)\n/exportmkts — outcomes CSV\n"
           "/history1d [days] — past daily (24h) markets: prices + outcomes (default 60)\n"
           "/history 4h|1h|15m [days] — past markets for that timeframe\n"
           "/history 5m [days] — past 5m markets, weekday mornings 8:30-11:30 ET only\n"
           "/history klines [days] — Binance 1m candles\n"
           "/tradehist 5m|15m|1h|1hall [days] — every past trade (price, size) — shows how much money can fit\n"
           "/mm · /mm all — paper market maker results · /mmexport — its fills + markets\n"
           "/mute · /unmute — hit messages off/on", force=True)


# ════════════════════════════════════════════════════════════════════════════
# PAPER MARKET MAKER — BTC up/down (5m + 15m). NO REAL ORDERS. Off unless MM_ENABLED=true.
#   * fair value of UP from Binance BTC (prices_ref) vs window open, using short-term vol
#   * pretend bids on UP and DOWN at fair - half_spread (several half-spreads at once = variants)
#   * fills simulated against the REAL Polymarket feed (websocket book + trades):
#       - a trade below our bid fills us; a trade AT our bid fills us only after the queue
#         that was ahead of us is used up; a resting ask at/below our bid fills us (cross)
#   * cancels/re-quotes take MM_LATENCY_SECS to happen — old quotes can still get hit meanwhile
#   * each side can fill up to MM_SIZE dollars per window; held to settlement; rebates estimated
# ════════════════════════════════════════════════════════════════════════════
import math
from collections import deque

MM_ENABLED = env_bool("MM_ENABLED", "false")
MM_ASSET = os.environ.get("MM_ASSET", "BTC").upper()
MM_TFS = [int(x) for x in os.environ.get("MM_TFS", "5,15").split(",") if x.strip()]
MM_SIZE = float(os.environ.get("MM_SIZE", "50"))                 # $ per side per window
# v2 (Oct 20): quote around the MARKET midpoint; Binance is only a danger signal (pull quotes).
# variant ids: 100+h = mid-h with guard, 200+h = mid-h without guard
# 300+h = mid-h with the ASK-PRESSURE guard only (v3, Oct 21): pull bids when the best ask on that
#         token has stepped DOWN within the last MM_ASKP_SECS (sellers pushing toward us)
MM_HALF = [float(x) for x in os.environ.get("MM_VARIANTS", "101,102,103,202,301,302").split(",") if x.strip()]
MM_ASKP_SECS = float(os.environ.get("MM_ASKP_SECS", "1.5"))
MM_ASKP_PAUSE = float(os.environ.get("MM_ASKP_PAUSE", "3"))
mm_ask_hist = {}     # token -> deque[(ts, best_ask)] (only when it changes)
MM_GUARD_CENTS = float(os.environ.get("MM_GUARD_CENTS", "3"))      # model vs market disagreement
MM_JUMP_BPS = float(os.environ.get("MM_JUMP_BPS", "4"))            # BTC move in last 3s that pulls quotes
MM_MAX_SPREAD = float(os.environ.get("MM_MAX_SPREAD", "5"))        # don't quote wider books
MM_PAUSE_SECS = float(os.environ.get("MM_PAUSE_SECS", "5"))


def mm_label(v):
    if v >= 300:
        return f"mid−{v - 300:g}¢ +ask guard"
    return f"mid−{v - 100:g}¢ +guard" if v < 200 else f"mid−{v - 200:g}¢ no guard"


def mm_note_ask(token):
    a = mm_best(token, "asks")
    h = mm_ask_hist.setdefault(token, deque(maxlen=50))
    if a is not None and (not h or h[-1][1] != a):
        h.append((time.time(), a))


def mm_ask_dropping(token, now):
    """has the best ask on this token stepped down within the last MM_ASKP_SECS?"""
    h = mm_ask_hist.get(token)
    if not h or len(h) < 2:
        return False
    pts = list(h)
    for (t0, a0), (t1, a1) in zip(pts, pts[1:]):
        if t1 >= now - MM_ASKP_SECS and a1 < a0:
            return True
    return False


def mm_jumped(now):
    """did BTC move more than MM_JUMP_BPS in the last ~3 seconds?"""
    px = prices_ref.get(MM_ASSET)
    old = [p for t, p in mm_btc if now - 4 <= t <= now - 2]
    if not px or not old:
        return False
    return abs(px / old[0] - 1) * 1e4 >= MM_JUMP_BPS
MM_LAT = float(os.environ.get("MM_LATENCY_SECS", "0.5"))          # cancel/replace delay
MM_STOP_SECS = float(os.environ.get("MM_STOP_SECS", "45"))        # stop quoting near the end
MM_START_SECS = float(os.environ.get("MM_START_SECS", "10"))      # wait after open
MM_FAIR_MIN, MM_FAIR_MAX = 8.0, 92.0                              # don't quote extreme markets
MM_REQUOTE = float(os.environ.get("MM_REQUOTE_CENTS", "1"))
MM_REBATE_SHARE = 0.20                                            # crypto maker rebate share
MM_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

mm_lock = threading.RLock()
mm_books = {}        # token -> {"bids": {px: sz}, "asks": {px: sz}, "ts": t}
mm_tokmap = {}       # token -> (tf, open_ts, "UP"/"DOWN", other_token)
mm_state = {}        # (variant, tf, open_ts) -> dict
mm_btc = deque(maxlen=3600)     # (ts, price) ~1/s
mm_stats = {"ws_msgs": 0, "ws_last": 0.0, "ws_lag": deque(maxlen=500), "trades": 0,
            "rest_polls": 0, "started": time.time(), "errors": 0}
mm_ws_holder = {"ws": None, "tokens": ()}
_mm_seen = {}


def mm_norm_cdf(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def mm_sigma():
    """per-second vol of BTC from ~1s samples over the last 15 min (floored)"""
    pts = [p for p in mm_btc if p[0] >= time.time() - 900]
    if len(pts) < 60:
        return 6e-5
    r = []
    for (t0, p0), (t1, p1) in zip(pts, pts[1:]):
        dt = t1 - t0
        if dt > 0 and p0 > 0 and p1 > 0:
            r.append((math.log(p1 / p0)) ** 2 / dt)
    if len(r) < 30:
        return 6e-5
    return max(2e-5, math.sqrt(sum(r) / len(r)))


def mm_fair_up(tf, open_ts, now):
    op = open_ref.get((tf, open_ts), {}).get(MM_ASSET)
    px = prices_ref.get(MM_ASSET)
    left = open_ts + tf * 60 - now
    if not op or not px or left <= 0:
        return None
    z = math.log(px / op) / (mm_sigma() * math.sqrt(left))
    return 100.0 * mm_norm_cdf(z)


# ── book / trade handling ────────────────────────────────────────────────────
def mm_book_set(token, bids, asks, ts=None):
    with mm_lock:
        mm_books[token] = {"bids": dict(bids), "asks": dict(asks), "ts": ts or time.time()}
        mm_note_ask(token)
        mm_check_cross(token)


def mm_book_change(token, side, px, sz):
    with mm_lock:
        b = mm_books.setdefault(token, {"bids": {}, "asks": {}, "ts": time.time()})
        lvl = b["bids"] if side == "BUY" else b["asks"]
        if sz <= 0:
            lvl.pop(px, None)
        else:
            lvl[px] = sz
        b["ts"] = time.time()
        if side != "BUY":
            mm_note_ask(token)
            mm_check_cross(token)


def mm_best(token, kind):
    b = mm_books.get(token)
    if not b or not b[kind]:
        return None
    return max(b[kind]) if kind == "bids" else min(b[kind])


def mm_on_trade(token, px, size, taker_side):
    """a real trade happened. Work out whose BIDS it hit: a taker SELL on this token hits bids on
    this token at px; a taker BUY on this token at px can match bids on the OTHER token at 100-px."""
    with mm_lock:
        mm_stats["trades"] += 1
        info = mm_tokmap.get(token)
        if not info:
            return
        tf, open_ts, side, other = info
        now = time.time()
        dk = (tf, open_ts, round(size, 2), round(px if side == "UP" else 100.0 - px, 2))
        if now - _mm_seen.get(dk, 0) < 1.5:
            return            # same match already seen on the other token
        _mm_seen[dk] = now
        if len(_mm_seen) > 20000:
            _mm_seen.clear()
        if taker_side == "SELL":
            hit_side, hit_px = side, px
        else:
            hit_side, hit_px = ("DOWN" if side == "UP" else "UP"), round(100.0 - px, 3)
        for (v, t2, o2), st in mm_state.items():
            if (t2, o2) != (tf, open_ts):
                continue
            q = st["quotes"].get(hit_side)
            if not q or q["px"] is None or time.time() < q["live_at"]:
                continue
            if hit_px < q["px"] - 1e-9:
                mm_fill(st, hit_side, q, q["left_sh"], "through")
            elif abs(hit_px - q["px"]) < 1e-9:
                q["queue"] -= size
                if q["queue"] < 0:
                    mm_fill(st, hit_side, q, min(q["left_sh"], -q["queue"]), "queue")
                    q["queue"] = 0.0


def mm_check_cross(token):
    """someone is now OFFERING at or below our bid on this token -> we'd have been filled"""
    info = mm_tokmap.get(token)
    if not info:
        return
    tf, open_ts, side, _o = info
    ask = mm_best(token, "asks")
    if ask is None:
        return
    for (v, t2, o2), st in mm_state.items():
        if (t2, o2) != (tf, open_ts):
            continue
        q = st["quotes"].get(side)
        if q and q["px"] is not None and time.time() >= q["live_at"] and ask <= q["px"] + 1e-9:
            mm_fill(st, side, q, q["left_sh"], "cross")


def mm_fill(st, side, q, shares, kind):
    if shares <= 1e-9:
        return
    px = q["px"]
    st["pos"][side] += shares
    st["cost"] += shares * px / 100.0
    st["rebate"] += MM_REBATE_SHARE * fee_for(shares, px / 100.0)
    q["left_sh"] -= shares
    now = time.time()
    fair = mm_fair_up(st["tf"], st["open_ts"], now)
    st["fills"].append((now, side, px, shares, kind))
    try:
        db_exec("INSERT INTO mm_fills (ts,variant,tf,open_ts,side,px,shares,kind,fair_up,btc,secs_left) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (now, st["variant"], st["tf"], st["open_ts"], side, px, round(shares, 3), kind,
                 round(fair, 2) if fair is not None else None, prices_ref.get(MM_ASSET),
                 round(st["open_ts"] + st["tf"] * 60 - now, 1)))
    except Exception as e:
        log.error(f"[MM] fill log: {e}")
    if q["left_sh"] <= 1e-6:
        q["px"] = None          # side done for this window
        q["done"] = True


# ── quoting loop ─────────────────────────────────────────────────────────────
def mm_new_state(v, tf, open_ts):
    return {"variant": v, "tf": tf, "open_ts": open_ts, "close_ts": open_ts + tf * 60,
            "pos": {"UP": 0.0, "DOWN": 0.0}, "cost": 0.0, "rebate": 0.0, "fills": [],
            "quotes": {s: {"px": None, "live_at": 0.0, "queue": 0.0, "left_sh": 0.0, "done": False,
                           "pending": None} for s in ("UP", "DOWN")}, "settled": False}


def mm_bid_size_at(token, px):
    b = mm_books.get(token)
    return float(b["bids"].get(px, 0.0)) if b else 0.0


def mm_step(now=None):
    now = time.time() if now is None else now
    with mm_lock:
        for tf in MM_TFS:
            open_ts, close_ts, left = window_times(tf, now)
            toks = resolve_tokens(MM_ASSET, tf, open_ts)
            if not toks:
                continue
            up_t, dn_t = toks
            mm_tokmap[up_t] = (tf, open_ts, "UP", dn_t)
            mm_tokmap[dn_t] = (tf, open_ts, "DOWN", up_t)
            fair = mm_fair_up(tf, open_ts, now)
            into = now - open_ts
            bu, au = mm_best(up_t, "bids"), mm_best(up_t, "asks")
            bd, ad = mm_best(dn_t, "bids"), mm_best(dn_t, "asks")
            mids = {}
            for side, b_, a_ in (("UP", bu, au), ("DOWN", bd, ad)):
                if b_ is not None and a_ is not None and a_ - b_ <= MM_MAX_SPREAD:
                    mids[side] = (b_ + a_) / 2.0
            mid_up = mids.get("UP")
            disagree = (fair is not None and mid_up is not None and abs(fair - mid_up) > MM_GUARD_CENTS)
            jump = mm_jumped(now)
            if jump or disagree:
                mm_stats["pause_until"] = max(mm_stats.get("pause_until", 0), now + MM_PAUSE_SECS)
            paused = now < mm_stats.get("pause_until", 0)
            for v in MM_HALF:
                key = (v, tf, open_ts)
                st = mm_state.get(key)
                if st is None:
                    if (tf, open_ts) not in open_ref or not open_ref[(tf, open_ts)].get(MM_ASSET):
                        continue   # we didn't see this window open: skip it
                    st = mm_state[key] = mm_new_state(v, tf, open_ts)
                guard = v < 200
                askguard = v >= 300
                h = v - 100 if guard else (v - 300 if askguard else v - 200)
                quoting = (into >= MM_START_SECS and left >= MM_STOP_SECS and mid_up is not None
                           and MM_FAIR_MIN <= mid_up <= MM_FAIR_MAX and not (guard and paused))
                for side, tok in (("UP", up_t), ("DOWN", dn_t)):
                    q = st["quotes"][side]
                    # apply a pending cancel/replace once its latency has passed
                    if q["pending"] and now >= q["pending"][1]:
                        newpx = q["pending"][0]
                        q["pending"] = None
                        if newpx is None or q["done"]:
                            q["px"] = None
                        else:
                            q["px"] = newpx
                            q["live_at"] = now
                            q["queue"] = mm_bid_size_at(tok, newpx)
                            spent = sum(f[3] * f[2] / 100.0 for f in st["fills"] if f[1] == side)
                            q["left_sh"] = max(0.0, (MM_SIZE - spent) / (newpx / 100.0))
                    if q["done"]:
                        continue
                    if not quoting:
                        if q["px"] is not None and not (q["pending"] and q["pending"][0] is None):
                            q["pending"] = (None, now + MM_LAT)
                        continue
                    if askguard and (mm_ask_dropping(tok, now) or now < q.get("block_until", 0)):
                        if mm_ask_dropping(tok, now):
                            q["block_until"] = now + MM_ASKP_PAUSE
                        cur = q["pending"][0] if q["pending"] else q["px"]
                        if cur is not None:
                            q["pending"] = (None, now + MM_LAT)
                        continue
                    if side not in mids:
                        want = None
                        cur = q["pending"][0] if q["pending"] else q["px"]
                        if cur is not None:
                            q["pending"] = (None, now + MM_LAT)
                        continue
                    want = mids[side] - h
                    best_ask = mm_best(tok, "asks")
                    if best_ask is not None:
                        want = min(want, best_ask - 1.0)      # post-only: never cross the ask
                    want = math.floor(want)
                    if want < 1:
                        want = None
                    cur = q["pending"][0] if q["pending"] else q["px"]
                    if want is None:
                        if cur is not None:
                            q["pending"] = (None, now + MM_LAT)
                    elif cur is None or abs(want - cur) >= MM_REQUOTE:
                        q["pending"] = (float(want), now + MM_LAT)


def mm_loop():
    last_sample = 0
    while True:
        try:
            time.sleep(0.2)
            now = time.time()
            px = prices_ref.get(MM_ASSET)
            if px and now - last_sample >= 1.0:
                mm_btc.append((now, px))
                last_sample = now
            mm_step(now)
        except Exception as e:
            mm_stats["errors"] += 1
            log.error(f"[MM] loop: {e}")
            time.sleep(1)


# ── feeds: Polymarket websocket (primary) + REST book polling (fallback) ─────
def mm_handle_msg(raw):
    try:
        data = json.loads(raw)
    except Exception:
        return
    items = data if isinstance(data, list) else [data]
    now = time.time()
    for m in items:
        if not isinstance(m, dict):
            continue
        mm_stats["ws_msgs"] += 1
        mm_stats["ws_last"] = now
        try:
            ts = float(m.get("timestamp", 0)) / 1000.0
            if ts > 1e9:
                mm_stats["ws_lag"].append(now - ts)
        except Exception:
            pass
        et = m.get("event_type")
        if et == "book":
            tok = str(m.get("asset_id"))
            bids = {round(float(x["price"]) * 100, 3): float(x["size"]) for x in (m.get("bids") or m.get("buys") or [])}
            asks = {round(float(x["price"]) * 100, 3): float(x["size"]) for x in (m.get("asks") or m.get("sells") or [])}
            mm_book_set(tok, bids, asks)
        elif et == "price_change":
            chg = m.get("price_changes") or m.get("changes") or []
            for c in chg:
                tok = str(c.get("asset_id") or m.get("asset_id"))
                mm_book_change(tok, str(c.get("side", "")).upper(), round(float(c["price"]) * 100, 3),
                               float(c.get("size", 0)))
        elif et == "last_trade_price":
            mm_on_trade(str(m.get("asset_id")), round(float(m["price"]) * 100, 3), float(m.get("size", 0)),
                        str(m.get("side", "")).upper())


def mm_ws_worker():
    while True:
        try:
            with mm_lock:
                toks = tuple(sorted(t for t, inf in mm_tokmap.items()
                                    if inf[1] + inf[0] * 60 > time.time() - 5))
            if not toks:
                time.sleep(2)
                continue
            mm_ws_holder["tokens"] = toks

            def on_open(ws):
                ws.send(json.dumps({"assets_ids": list(toks), "type": "market"}))

            def on_message(ws, msg):
                mm_handle_msg(msg)
                with mm_lock:
                    cur = tuple(sorted(t for t, inf in mm_tokmap.items()
                                       if inf[1] + inf[0] * 60 > time.time() - 5))
                if cur != toks:          # new windows -> resubscribe
                    ws.close()

            ws = websocket.WebSocketApp(MM_WS_URL, on_open=on_open, on_message=on_message)
            mm_ws_holder["ws"] = ws
            ws.run_forever(ping_interval=15, ping_timeout=10)
        except Exception as e:
            mm_stats["errors"] += 1
            log.warning(f"[MM] ws: {e}")
        time.sleep(1)


def mm_rest_worker():
    """if the websocket goes quiet, keep books fresh from REST (no trade prints -> cross fills only)"""
    while True:
        try:
            time.sleep(1.0)
            if time.time() - mm_stats["ws_last"] < 5:
                continue
            with mm_lock:
                toks = [t for t, inf in mm_tokmap.items() if inf[1] + inf[0] * 60 > time.time()]
            for tok in toks:
                b = get_book(tok)
                if b:
                    mm_stats["rest_polls"] += 1
                    mm_book_set(tok, {p: s for p, s in b["bids"]}, {p: s for p, s in b["asks"]})
        except Exception as e:
            mm_stats["errors"] += 1
            log.error(f"[MM] rest: {e}")


# ── settlement & reporting ───────────────────────────────────────────────────
_mm_outcomes = {}


def mm_settle_once(now=None):
    now = time.time() if now is None else now
    with mm_lock:
        due = [(k, st) for k, st in mm_state.items() if not st["settled"] and now >= st["close_ts"] + 20]
    for k, st in due:
        key = (st["tf"], st["open_ts"])
        oc = _mm_outcomes.get(key)
        if oc is None:
            oc = fetch_outcome(MM_ASSET, st["tf"], st["open_ts"])
            if oc is None:
                if now > st["close_ts"] + 1800:
                    oc = "VOID"
                else:
                    continue
            _mm_outcomes[key] = oc
        with mm_lock:
            payout = st["pos"]["UP"] if oc == "UP" else st["pos"]["DOWN"] if oc == "DOWN" else st["cost"]
            pnl = payout - st["cost"]
            st["settled"] = True
        if st["fills"]:
            db_exec("INSERT OR REPLACE INTO mm_mkts VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    (st["variant"], st["tf"], st["open_ts"], oc, round(st["pos"]["UP"], 3),
                     round(st["pos"]["DOWN"], 3), round(st["cost"], 4), round(payout, 4),
                     round(st["rebate"], 4), round(pnl + st["rebate"], 4), len(st["fills"])))
        with mm_lock:
            mm_state.pop(k, None)
    # forget old token mappings / books
    with mm_lock:
        for t in [t for t, inf in mm_tokmap.items() if inf[1] + inf[0] * 60 < now - 3600]:
            mm_tokmap.pop(t, None)
            mm_books.pop(t, None)


def mm_settle_worker():
    while True:
        try:
            time.sleep(10)
            mm_settle_once()
        except Exception as e:
            mm_stats["errors"] += 1
            log.error(f"[MM] settle: {e}")


def mm_init_db():
    with db_lock:
        conn = db()
        conn.execute("""CREATE TABLE IF NOT EXISTS mm_fills (id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts REAL, variant REAL, tf INTEGER, open_ts INTEGER, side TEXT, px REAL, shares REAL,
            kind TEXT, fair_up REAL, btc REAL, secs_left REAL)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS mm_mkts (variant REAL, tf INTEGER, open_ts INTEGER,
            outcome TEXT, sh_up REAL, sh_dn REAL, cost REAL, payout REAL, rebate REAL, pnl REAL,
            fills INTEGER, PRIMARY KEY (variant, tf, open_ts))""")
        conn.commit()
        conn.close()


def mm_text(scope="today"):
    since = today_start() if scope == "today" else 0
    rows = db_query("SELECT * FROM mm_mkts WHERE open_ts >= ? AND variant >= 100 ORDER BY open_ts", (since,))
    lag = sorted(mm_stats["ws_lag"])
    lag_s = f"{lag[len(lag) // 2]:.2f}s" if lag else "n/a"
    lines = [f"🏦 <b>PAPER MARKET MAKER</b> · {MM_ASSET} {'/'.join(label_for(t) for t in MM_TFS)} · "
             f"${MM_SIZE:g}/side · latency {MM_LAT:g}s · {'today' if scope == 'today' else 'all time'}",
             f"feed: {mm_stats['ws_msgs']} ws msgs · {mm_stats['trades']} trades seen · median lag {lag_s} · "
             f"rest polls {mm_stats['rest_polls']} · errors {mm_stats['errors']} · v2: quotes around market mid, "
             f"pulls on >{MM_GUARD_CENTS:g}¢ disagreement or >{MM_JUMP_BPS:g}bp BTC jump · v3 ask guard: pulls when sellers step down"]
    if not rows:
        lines.append("no settled markets yet")
        return "\n".join(lines)
    for v in MM_HALF:
        for tf in MM_TFS:
            rs = [r for r in rows if r["variant"] == v and r["tf"] == tf]
            if not rs:
                continue
            both = sum(1 for r in rs if r["sh_up"] > 0 and r["sh_dn"] > 0)
            pnl = sum(r["pnl"] for r in rs)
            reb = sum(r["rebate"] for r in rs)
            vol = sum(r["cost"] for r in rs)
            wins = sum(1 for r in rs if r["pnl"] > 0)
            lines.append(f"{mm_label(v)} {label_for(tf)}: {len(rs)} mkts · both sides {both / len(rs) * 100:.0f}% · "
                         f"traded ${vol:.0f} · {money(pnl)} (rebates {money(reb)}) · {wins}/{len(rs)} mkts +")
    return "\n".join(lines)


def mm_start():
    if not MM_ENABLED:
        return
    mm_init_db()
    for fn in (mm_loop, mm_ws_worker, mm_rest_worker, mm_settle_worker):
        threading.Thread(target=fn, daemon=True).start()
    log.info(f"[MM] paper market maker v2 ON: {MM_ASSET} {MM_TFS} variants {MM_HALF} size ${MM_SIZE}")



def main():
    if not WEBSOCKET_AVAILABLE:
        log.error("websocket-client not installed")
        return
    init_db()
    threading.Thread(target=binance_ref_worker, daemon=True).start()
    entry = " · ".join(f"{label_for(t)}: first {ENTRY_SECS.get(t, 0) / 60:g}m" for t in TFS)
    tg(f"🎰 <b>PAPER LONGSHOT v2</b> — no money\n"
       f"buys the {MIN_CENTS:.0f}–{MAX_CENTS:.0f}¢ side · {entry}\n"
       f"real book fills · fees applied · hold AND ladder tracked\n"
       f"shadow scans {'ON' if SCAN_ENABLED else 'OFF'} · ${STAKE:g}/trade · "
       f"{len(COINS)} coins\n/help for commands", force=True)
    time.sleep(3)
    threading.Thread(target=trader, daemon=True).start()
    threading.Thread(target=grader, daemon=True).start()
    if SCAN_ENABLED:
        threading.Thread(target=scanner, daemon=True).start()
    try:
        mm_start()
    except Exception as e:
        log.error(f"[MM] start failed: {e}")
    while True:
        try:
            handle_commands()
        except Exception as e:
            log.error(f"main: {e}")
        time.sleep(1)


if __name__ == "__main__":
    main()
