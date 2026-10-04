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
from datetime import datetime, timezone
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
}
SCAN_VWAP_MAX_CENTS = float(os.environ.get("SCAN_VWAP_MAX_CENTS", "25"))
SCAN_KEEP_DAYS = float(os.environ.get("SCAN_KEEP_DAYS", "30"))
# price bands tracked on the side (shadow only — the bot still TRADES MIN..MAX)
BANDS = [tuple(float(v) for v in b.split("-")) for b in
         os.environ.get("BANDS", "1-5,5-10,10-15,15-20").split(",") if "-" in b]
# how often to look for entries / walk ladders, per timeframe
POLL = {5: 2.0, 15: 3.0, 60: 8.0, 240: 15.0}
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
    return "4h" if tf == 240 else "1h" if tf == 60 else f"{tf}m"


def parse_tf(arg):
    a = (arg or "").strip().lower()
    return {"5": 5, "5m": 5, "15": 15, "15m": 15, "60": 60, "1h": 60,
            "240": 240, "4h": 240}.get(a)


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
        rows.append((now, a, tf, open_ts, round(tf * 60 - secs_left, 1), ua, up["bid"],
                     da, dn["bid"], cheap, bk["ask"], vw, sh, depth, mv))
        mark_due(a, tf, open_ts, close_ts)
    if rows:
        db_exec("""INSERT INTO scans (ts,asset,tf,open_ts,secs_into,up_ask,up_bid,dn_ask,
                   dn_bid,cheap,cheap_ask,cheap_vwap,cheap_shares,cheap_depth,move_pct)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", rows, many=True)


def scanner():
    last_prune = 0
    while True:
        try:
            time.sleep(1.0)
            for tf in TFS:
                open_ts, close_ts, secs_left = window_times(tf)
                into = tf * 60 - secs_left
                b = int(into // SCAN_SECS[tf])
                key = (tf, open_ts, b)
                if key in scan_done or secs_left < 2:
                    continue
                scan_done.add(key)
                do_scan(tf, open_ts, close_ts, secs_left)
            if time.time() - last_prune > 3600:
                last_prune = time.time()
                old = time.time() - 6 * 3600
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
                    fresh = tf * 60 - secs_left <= 5
                    open_ref[(tf, open_ts)] = dict(prices_ref) if fresh else {}
                    old = now - 8 * 3600
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
           "/mute · /unmute — hit messages off/on", force=True)


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
    while True:
        try:
            handle_commands()
        except Exception as e:
            log.error(f"main: {e}")
        time.sleep(1)


if __name__ == "__main__":
    main()
