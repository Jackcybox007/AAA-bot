# market.py — full version with universe/server/user reports and market_stats persistence

import os
import time
import math
import sqlite3
from dataclasses import dataclass
from typing import Any, List, Optional, Tuple, Dict
from datetime import datetime, timezone
from config import config

# ---- paths ----
PRUN_DB = config.DB_PATH
BOT_DB  = config.BOT_DB

# ---- report paths ----
REPORT_BASE        = os.path.join("tmp", "reports")
REPORT_UNI_PATH    = os.path.join(REPORT_BASE, "universe", "report.md")
REPORT_SERVER_PATH = os.path.join(REPORT_BASE, "server",   "report.md")
REPORT_STATE_DB = os.path.join(os.path.dirname(config.BOT_DB), "report_state.db")
REPORT_USERS_DIR   = os.path.join(REPORT_BASE, "users")

# ---- CX sets ----
CX_REAL = ("NC1", "NC2", "CI1", "CI2", "IC1", "AI1")
CX_ALL  = ("UN",) + CX_REAL

# ---- windows and thresholds (ms) ----
DAY_MS     = 24 * 3600 * 1000
WIN7D_MS   = 7 * DAY_MS
WIN30D_MS  = 30 * DAY_MS
Z_THRESHOLD = 2.0  # price anomaly flag

# =========================
# util
# =========================
def ensure_dirs():
    os.makedirs(os.path.dirname(REPORT_UNI_PATH), exist_ok=True)
    os.makedirs(os.path.dirname(REPORT_SERVER_PATH), exist_ok=True)
    os.makedirs(REPORT_USERS_DIR, exist_ok=True)

def open_db(path: str) -> sqlite3.Connection:
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    return con

def open_report_state_db() -> sqlite3.Connection:
    con = sqlite3.connect(REPORT_STATE_DB, timeout=5.0)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute("PRAGMA busy_timeout=5000")
    con.row_factory = sqlite3.Row
    return con

def table_exists(con: sqlite3.Connection, name: str) -> bool:
    row = con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone()
    return bool(row)

def now_ms() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)

def now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat(timespec="seconds")

def fmt(x: Optional[float], nd: int = 2) -> str:
    if x is None:
        return "—"
    return f"{x:.{nd}f}"

def _pct(new: Optional[float], old: Optional[float]) -> Optional[float]:
    if new is None or old is None or old == 0:
        return None
    return 100.0 * (new - old) / old

def _mean_std(vals: List[float]) -> Tuple[Optional[float], Optional[float]]:
    if not vals:
        return None, None
    n = len(vals)
    mu = sum(vals) / n
    if n < 2:
        return mu, None
    var = sum((v - mu) ** 2 for v in vals) / (n - 1)
    return mu, var ** 0.5

def _compile_universe_rows() -> List[Any]:
    prun_con = open_db(PRUN_DB)
    try:
        ensure_snapshot_table(prun_con)
        ensure_market_stats_table(prun_con)
        uni_quotes = fetch_all_quotes(prun_con, list(CX_ALL))
        uni_rows: List[Any] = []
        for q in uni_quotes:
            sr = build_row(prun_con, q)
            if sr:
                insert_snapshot(prun_con, sr)
                uni_rows.append(sr)
        prun_con.commit()
        persist_stats(prun_con, now_ms(), uni_rows)
        return uni_rows
    finally:
        prun_con.close()

def _rget(obj, key, default=None):
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)

def _row_identity(sr) -> Tuple[str, str]:
    return str(_rget(sr, "ticker")).upper(), str(_rget(sr, "cx")).upper()

def _row_prices(sr) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    bid = _rget(sr, "best_bid")
    ask = _rget(sr, "best_ask")
    bid = float(bid) if bid is not None else None
    ask = float(ask) if ask is not None else None
    mid = ((bid + ask) / 2.0) if (bid is not None and ask is not None) else None
    spr = (ask - bid) if (bid is not None and ask is not None) else None
    return bid, ask, mid, spr

def _eq(a: Optional[float], b: Optional[float]) -> bool:
    if a is None and b is None:
        return True
    if (a is None) != (b is None):
        return False
    return math.isclose(float(a), float(b), rel_tol=0.0, abs_tol=1e-9)

def _filter_changed_for_scope(rows: List[Any], scope: str, user_id: Optional[str]) -> List[Any]:
    uid = "" if user_id is None else str(user_id)

    con = open_report_state_db()
    try:
        ensure_report_state_table(con)

        prev = {}
        def _load_prev():
            cur = con.execute(
                "SELECT ticker,cx,last_bid,last_ask FROM report_state WHERE scope=? AND user_id=?",
                (scope, uid)
            )
            return {(str(r["ticker"]).upper(), str(r["cx"]).upper()): (r["last_bid"], r["last_ask"]) for r in cur.fetchall()}

        prev = _sql_retry(_load_prev)

        changed: List[Any] = []
        now = now_ms()
        for sr in rows:
            t, x = _row_identity(sr)
            bid, ask, mid, spr = _row_prices(sr)
            p_bid, p_ask = prev.get((t, x), (None, None))
            if not (_eq(bid, p_bid) and _eq(ask, p_ask)):
                changed.append(sr)

            def _upsert():
                con.execute("""
                    INSERT INTO report_state(scope,user_id,ticker,cx,last_bid,last_ask,last_mid,last_spread,last_ts)
                    VALUES (?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(scope,user_id,ticker,cx) DO UPDATE SET
                        last_bid=excluded.last_bid,
                        last_ask=excluded.last_ask,
                        last_mid=excluded.last_mid,
                        last_spread=excluded.last_spread,
                        last_ts=excluded.last_ts
                """, (scope, uid, t, x, bid, ask, mid, spr, now))
                con.commit()
            _sql_retry(_upsert)

        return changed
    finally:
        con.close()

# =========================
# storage: snapshots and stats
# =========================
def ensure_snapshot_conflict_target(con: sqlite3.Connection):
    # remove dup rows so the UNIQUE can be created
    con.execute("""
        DELETE FROM market_snapshot
        WHERE rowid NOT IN (
          SELECT MIN(rowid) FROM market_snapshot GROUP BY cx, ticker
        )
    """)
    # ensure a conflict target for ON CONFLICT(cx,ticker)
    con.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_market_snapshot_cx_ticker ON market_snapshot(cx, ticker)")

def ensure_snapshot_tsiso_backfill(con: sqlite3.Connection):
    cols = [r[1].lower() for r in con.execute("PRAGMA table_info(market_snapshot)").fetchall()]
    if "ts_iso" not in cols:
        con.execute("ALTER TABLE market_snapshot ADD COLUMN ts_iso TEXT")
    # backfill any NULL/empty rows
    rows = con.execute("SELECT rowid, ts_ms FROM market_snapshot WHERE ts_iso IS NULL OR ts_iso=''").fetchall()
    for rowid, ts in rows:
        iso = datetime.fromtimestamp(int(ts)/1000, tz=timezone.utc).isoformat(timespec="seconds")
        con.execute("UPDATE market_snapshot SET ts_iso=? WHERE rowid=?", (iso, rowid))
    con.commit()

# market.py — ensure table in the NEW DB
def ensure_report_state_table(con: sqlite3.Connection) -> None:
    con.execute("""
    CREATE TABLE IF NOT EXISTS report_state (
        scope TEXT NOT NULL,
        user_id TEXT NOT NULL,
        ticker TEXT NOT NULL,
        cx TEXT NOT NULL,
        last_bid REAL,
        last_ask REAL,
        last_mid REAL,
        last_spread REAL,
        last_ts INTEGER NOT NULL,
        PRIMARY KEY (scope, user_id, ticker, cx)
    )
    """)
    con.commit()


def ensure_snapshot_table(con: sqlite3.Connection):
    con.execute("""
    CREATE TABLE IF NOT EXISTS market_snapshot(
      ts_ms  INTEGER NOT NULL,
      ts_iso TEXT    NOT NULL,   -- NEW
      cx     TEXT    NOT NULL,
      ticker TEXT    NOT NULL,
      pb     REAL,
      pa     REAL,
      spread REAL,
      mid    REAL,
      PP7    REAL,
      PP30   REAL,
      dev7   REAL,
      dev30  REAL,
      z7     REAL
    )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_snapshot_time ON market_snapshot(ts_ms)")
    # conflict target for ON CONFLICT(cx,ticker)
    con.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_market_snapshot_cx_ticker ON market_snapshot(cx, ticker)")


def ensure_market_stats_table(con: sqlite3.Connection):
    con.execute("""
        CREATE TABLE IF NOT EXISTS market_stats (
            ts_ms    INTEGER NOT NULL,
            category TEXT    NOT NULL,  -- 'top_movers','best_pct_spread','arb_margin','maker_profit'
            rank     INTEGER NOT NULL,
            cx       TEXT,
            ticker   TEXT,
            value    REAL,              -- main score (e.g., % or z or margin%)
            aux      REAL,              -- secondary (e.g., profit_per_unit)
            note     TEXT,              -- e.g., "NC1->IC1"
            PRIMARY KEY (ts_ms, category, rank, cx, ticker)
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_stats_cat_time ON market_stats(category, ts_ms)")

def ensure_report_state_table(con: sqlite3.Connection) -> None:
    con.execute("""
    CREATE TABLE IF NOT EXISTS report_state (
        scope TEXT NOT NULL,              -- 'server' or 'user'
        user_id TEXT NOT NULL,            -- '' for server
        ticker TEXT NOT NULL,
        cx TEXT NOT NULL,
        last_bid REAL,
        last_ask REAL,
        last_mid REAL,
        last_spread REAL,
        last_ts INTEGER NOT NULL,
        PRIMARY KEY (scope, user_id, ticker, cx)
    )
    """)
    con.commit()

def insert_snapshot(con: sqlite3.Connection, row: "SnapshotRow"):
    con.execute("""
    INSERT INTO market_snapshot(ts_ms,ts_iso,cx,ticker,pb,pa,spread,mid,PP7,PP30,dev7,dev30,z7)
    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
    ON CONFLICT(cx,ticker) DO UPDATE SET
      ts_ms=excluded.ts_ms,
      ts_iso=excluded.ts_iso,
      pb=excluded.pb, pa=excluded.pa, spread=excluded.spread, mid=excluded.mid,
      PP7=excluded.PP7, PP30=excluded.PP30, dev7=excluded.dev7, dev30=excluded.dev30, z7=excluded.z7
    """, (row.ts_ms, row.ts_iso, row.cx, row.ticker, row.pb, row.pa, row.spread, row.mid,
          row.PP7, row.PP30, row.dev7, row.dev30, row.z7))

def fetch_last_snapshot(con: sqlite3.Connection, cx: str, ticker: str) -> Optional[sqlite3.Row]:
    return con.execute("SELECT pb,pa,spread FROM market_snapshot WHERE cx=? AND ticker=?", (cx, ticker)).fetchone()

# market.py — ADD tiny retry helper
def _sql_retry(fn, retries: int = 6, base_sleep: float = 0.15):
    for i in range(retries):
        try:
            return fn()
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower():
                time.sleep(base_sleep * (i + 1))
                continue
            raise

# =========================
# inputs from prun.db
# =========================
def fetch_all_quotes(con: sqlite3.Connection, cx_list: List[str]) -> List[sqlite3.Row]:
    if not cx_list:
        return []
    qx = ",".join("?" for _ in cx_list)
    sql = f"""
        SELECT cx,ticker,best_bid,best_ask,PP7,PP30,ts
        FROM prices
        WHERE cx IN ({qx})
    """
    return list(con.execute(sql, list(cx_list)).fetchall())

def fetch_mid_history(con: sqlite3.Connection, cx: str, ticker: str, win_ms: int) -> List[float]:
    """
    Pull mids from price_history for the last window.
    """
    cutoff = now_ms() - win_ms
    rows = con.execute(
        "SELECT bid,ask FROM price_history WHERE cx=? AND ticker=? AND ts>=? ORDER BY ts ASC",
        (cx, ticker, cutoff)
    ).fetchall()
    mids = []
    for r in rows:
        b = r["bid"]; a = r["ask"]
        if b is None or a is None:
            continue
        mids.append((float(b) + float(a)) / 2.0)
    return mids

# =========================
# domain model
# =========================
@dataclass
class SnapshotRow:
    ts_iso: str
    ts_ms: int
    cx: str
    ticker: str
    pb: Optional[float]
    pa: Optional[float]
    spread: Optional[float]
    mid: Optional[float]
    pct_bid: Optional[float]
    pct_ask: Optional[float]
    delta_spread: Optional[float]
    PP7: Optional[float]
    PP30: Optional[float]
    dev7: Optional[float]
    dev30: Optional[float]
    z7: Optional[float]
    price_anomaly: int

    def mover_score(self) -> float:
        a = abs(self.pct_bid) if self.pct_bid is not None else 0.0
        b = abs(self.pct_ask) if self.pct_ask is not None else 0.0
        return max(a, b)

def build_row(prun_con: sqlite3.Connection, current: sqlite3.Row) -> Optional[SnapshotRow]:
    cx, tk = str(current["cx"]).upper(), str(current["ticker"]).upper()
    pb = float(current["best_bid"]) if current["best_bid"] is not None else None
    pa = float(current["best_ask"]) if current["best_ask"] is not None else None
    spread = (pa - pb) if (pb is not None and pa is not None) else None
    mid = ((pb + pa) / 2.0) if (pb is not None and pa is not None) else None

    PP7  = float(current["PP7"])  if ("PP7"  in current.keys() and current["PP7"]  is not None) else None
    PP30 = float(current["PP30"]) if ("PP30" in current.keys() and current["PP30"] is not None) else None

    # fill missing rolling means from history if needed
    hist7  = []
    hist30 = []
    if PP7 is None or PP30 is None:
        hist7  = fetch_mid_history(prun_con, cx, tk, WIN7D_MS)
        hist30 = fetch_mid_history(prun_con, cx, tk, WIN30D_MS)
        if PP7 is None:
            mu7, _ = _mean_std(hist7)
            PP7 = mu7
        if PP30 is None:
            mu30, _ = _mean_std(hist30)
            PP30 = mu30
    if not hist7:
        hist7 = fetch_mid_history(prun_con, cx, tk, WIN7D_MS)
    mean7, std7 = _mean_std(hist7)

    prev = fetch_last_snapshot(prun_con, cx, tk)
    pct_bid = _pct(pb, float(prev["pb"])) if (prev and prev["pb"] is not None and pb is not None) else None
    pct_ask = _pct(pa, float(prev["pa"])) if (prev and prev["pa"] is not None and pa is not None) else None
    delta_spread = (spread - float(prev["spread"])) if (spread is not None and prev and prev["spread"] is not None) else None

    dev7 = (mid - PP7) if (mid is not None and PP7 is not None) else None
    dev30 = (mid - PP30) if (mid is not None and PP30 is not None) else None
    z7 = ((mid - mean7) / std7) if (mid is not None and mean7 is not None and std7 and std7 > 0) else None
    panom = 1 if (z7 is not None and abs(z7) >= Z_THRESHOLD) else 0

    return SnapshotRow(
        ts_iso=now_iso(), ts_ms=now_ms(),
        cx=cx, ticker=tk,
        pb=pb, pa=pa, spread=spread, mid=mid,
        pct_bid=pct_bid, pct_ask=pct_ask, delta_spread=delta_spread,
        PP7=PP7, PP30=PP30, dev7=dev7, dev30=dev30,
        z7=z7, price_anomaly=panom
    )

# =========================
# stats
# =========================
def _pct_spread(r: SnapshotRow) -> Optional[float]:
    if r.spread is None or r.mid is None or r.mid <= 0:
        return None
    return 100.0 * r.spread / r.mid

def compute_arbitrage(rows: List[SnapshotRow]) -> List[Tuple[str, str, str, float, float]]:
    """
    Returns list of (ticker, from_cx, to_cx, margin_pct, profit_per_unit).
    Uses real CXs only to avoid UN double counting.
    """
    by_ticker: Dict[str, List[SnapshotRow]] = {}
    for r in rows:
        if r.cx in CX_REAL:
            by_ticker.setdefault(r.ticker, []).append(r)

    out: List[Tuple[str, str, str, float, float]] = []
    for tk, lst in by_ticker.items():
        min_ask_row = min((x for x in lst if x.pa is not None), key=lambda x: x.pa, default=None)
        max_bid_row = max((x for x in lst if x.pb is not None), key=lambda x: x.pb, default=None)
        if not min_ask_row or not max_bid_row:
            continue
        if max_bid_row.pb is None or min_ask_row.pa is None:
            continue
        ppu = max_bid_row.pb - min_ask_row.pa
        if min_ask_row.pa <= 0:
            continue
        margin = 100.0 * ppu / min_ask_row.pa
        out.append((tk, min_ask_row.cx, max_bid_row.cx, margin, ppu))
    out.sort(key=lambda t: t[3], reverse=True)
    return out

def compute_maker_profit(rows: List[SnapshotRow]) -> List[Tuple[str, str, float, float]]:
    """
    For each CX, maker profit = half the spread.
    Returns (ticker, cx, maker_profit_pct, maker_profit_unit).
    """
    out: List[Tuple[str, str, float, float]] = []
    for r in rows:
        if r.cx not in CX_REAL or r.spread is None or r.mid is None or r.mid <= 0:
            continue
        unit = r.spread / 2.0
        pct = 100.0 * unit / r.mid
        out.append((r.ticker, r.cx, pct, unit))
    out.sort(key=lambda t: t[2], reverse=True)
    return out

def persist_stats(con: sqlite3.Connection, ts_ms: int, rows: List[SnapshotRow]):
    ensure_market_stats_table(con)

    # movers
    movers = sorted(rows, key=lambda r: r.mover_score(), reverse=True)[:50]

    # tightest % spread
    pct_spread = [(r, _pct_spread(r)) for r in rows if _pct_spread(r) is not None]
    pct_spread.sort(key=lambda t: t[1])  # tightest first
    pct_spread = pct_spread[:50]

    # arbitrage
    arbs = compute_arbitrage(rows)[:50]

    # maker profit
    makers = compute_maker_profit(rows)[:50]

    cur = con.cursor()
    rank = 0
    for r in movers:
        rank += 1
        cur.execute(
            "INSERT OR REPLACE INTO market_stats(ts_ms,category,rank,cx,ticker,value,aux,note) VALUES(?,?,?,?,?,?,?,?)",
            (ts_ms, "top_movers", rank, r.cx, r.ticker, r.mover_score(), None, None),
        )
    rank = 0
    for r, val in pct_spread:
        rank += 1
        cur.execute(
            "INSERT OR REPLACE INTO market_stats(ts_ms,category,rank,cx,ticker,value,aux,note) VALUES(?,?,?,?,?,?,?,?)",
            (ts_ms, "best_pct_spread", rank, r.cx, r.ticker, val, r.spread, None),
        )
    rank = 0
    for tk, from_cx, to_cx, m_pct, ppu in arbs:
        rank += 1
        cur.execute(
            "INSERT OR REPLACE INTO market_stats(ts_ms,category,rank,cx,ticker,value,aux,note) VALUES(?,?,?,?,?,?,?,?)",
            (ts_ms, "arb_margin", rank, f"{from_cx}->{to_cx}", tk, m_pct, ppu, None),
        )
    rank = 0
    for tk, cx, mpct, unit in makers:
        rank += 1
        cur.execute(
            "INSERT OR REPLACE INTO market_stats(ts_ms,category,rank,cx,ticker,value,aux,note) VALUES(?,?,?,?,?,?,?,?)",
            (ts_ms, "maker_profit", rank, cx, tk, mpct, unit, None),
        )
    con.commit()

# =========================
# rendering
# =========================
def _fmt_price(x: Optional[float]) -> str:
    return f"{x:.2f}" if x is not None else "—"

def _fmt_pct_paren(x: Optional[float]) -> str:
    # Show like "(0.0%)", treat None as 0.0%
    v = 0.0 if x is None else x
    return f"({v:.1f}%)"

def _fmt_delta_spread(x: Optional[float]) -> str:
    # show sign only when negative; positive prints as plain 0.00 like examples
    if x is None:
        return "0.00"
    s = f"{x:.2f}"
    return s if x >= 0 else f"{s}"

def _fmt_z(x: Optional[float]) -> str:
    return f"{x:.2f}" if x is not None else "—"

def _discord_time_tags(epoch_s: int) -> str:
    return f"<t:{epoch_s}:f> (<t:{epoch_s}:R>)"

def _top_movers_subset(rows: List[SnapshotRow], k: int = 10) -> List[SnapshotRow]:
    return sorted(rows, key=lambda r: r.mover_score(), reverse=True)[:k]

def _spread_tightening_subset(rows: List[SnapshotRow], k: int = 10) -> List[SnapshotRow]:
    # smallest (most negative) Δspr first
    with_delta = [r for r in rows if r.delta_spread is not None]
    with_delta.sort(key=lambda r: r.delta_spread)  # ascending
    return with_delta[:k]

def render_watchlist_report(rows: List[SnapshotRow]) -> str:
    now_epoch = int(time.time())
    lines: List[str] = []
    lines.append("**Market Report**")
    lines.append(f"_As of {_discord_time_tags(now_epoch)}_")
    lines.append("")
    # Top movers
    lines.append("**Top movers**")
    tm = _top_movers_subset(rows, k=10)
    for r in tm:
        lines.append(
            f"- **{r.ticker} {r.cx}** | bid {_fmt_price(r.pb)} {_fmt_pct_paren(r.pct_bid)} | "
            f"ask {_fmt_price(r.pa)} {_fmt_pct_paren(r.pct_ask)} | spr {_fmt_price(r.spread)}"
        )
    lines.append("")
    # Spread tightening
    lines.append("**Spread tightening**")
    st = _spread_tightening_subset(rows, k=10)
    for r in st:
        lines.append(
            f"- **{r.ticker} {r.cx}** | spr {_fmt_price(r.spread)} | Δspr {_fmt_delta_spread(r.delta_spread)}"
        )
    lines.append("")
    # Watchlist details
    lines.append("**Watchlist details**")
    # stable order: by ticker, then cx
    for r in sorted(rows, key=lambda x: (x.ticker, x.cx)):
        lines.append(
            f"- **{r.ticker} {r.cx}** | b {_fmt_price(r.pb)} a {_fmt_price(r.pa)} | "
            f"spr {_fmt_price(r.spread)} | mid {_fmt_price(r.mid)} | "
            f"Δb {(_fmt_pct_paren(r.pct_bid))[1:-1]} Δa {(_fmt_pct_paren(r.pct_ask))[1:-1]} | "
            f"z7 {_fmt_z(r.z7)}"
        )
    lines.append("")
    # Footprint
    lines.append("**Footprint**")
    lines.append("- b = best bid; a = best ask")
    lines.append("- spr = spread (ask−bid); Δspr = change in spread vs last run")
    lines.append("- mid = (bid+ask)/2")
    lines.append("- Δb, Δa = percent change since last run")
    lines.append("- PP7 = 7-day mean of mid")
    lines.append("- dev7 = mid − PP7")
    lines.append("- z7 = z-score vs 7-day mid distribution")
    return "\n".join(lines)

def render_rows_section(title: str, rows: List[SnapshotRow], limit: int = 25) -> List[str]:
    out: List[str] = []
    out.append(f"### {title}")
    out.append("")
    out.append("| Ticker | CX | Bid | Ask | Mid | Spread | %Bid | %Ask | dev7 | dev30 | z7 |")
    out.append("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for r in rows[:limit]:
        out.append(
            f"| {r.ticker} | {r.cx} | {fmt(r.pb)} | {fmt(r.pa)} | {fmt(r.mid)} | {fmt(r.spread)} | "
            f"{fmt(r.pct_bid,2) if r.pct_bid is not None else '—'} | {fmt(r.pct_ask,2) if r.pct_ask is not None else '—'} | "
            f"{fmt(r.dev7)} | {fmt(r.dev30)} | {fmt(r.z7,2) if r.z7 is not None else '—'} |"
        )
    out.append("")
    return out

def render_stats_sections(rows: List[SnapshotRow]) -> str:
    def _pp(x: Optional[float]) -> str:
        return f"{x:.1f}%" if x is not None else "—"

    movers = sorted(rows, key=lambda r: r.mover_score(), reverse=True)[:10]
    tight = [(r, _pct_spread(r)) for r in rows if _pct_spread(r) is not None]
    tight.sort(key=lambda t: t[1])  # tightest first
    tight = tight[:10]
    arbs = compute_arbitrage(rows)[:10]
    makers = compute_maker_profit(rows)[:10]

    lines: List[str] = []
    lines.append("## Highlights")
    lines.append("")

    if movers:
        lines.append("**Top movers**")
        for r in movers:
            lines.append(
                f"- **{r.ticker} {r.cx}** | bid {fmt(r.pb)} ({_pp(r.pct_bid)}) | "
                f"ask {fmt(r.pa)} ({_pp(r.pct_ask)}) | spr {fmt(r.spread)}"
            )
        lines.append("")

    if tight:
        lines.append("**Tightest % spreads**")
        for r, ps in tight:
            lines.append(
                f"- **{r.ticker} {r.cx}** | %spr {fmt(ps,2)} | spr {fmt(r.spread)} | mid {fmt(r.mid)}"
            )
        lines.append("")

    if arbs:
        lines.append("**Best cross-CX arbitrage (gross)**")
        for tk, from_cx, to_cx, m_pct, ppu in arbs:
            lines.append(f"- **{tk}** {from_cx}→{to_cx} | ppu {fmt(ppu)} | margin {fmt(m_pct,2)}%")
        lines.append("")

    if makers:
        lines.append("**Maker capture (half-spread)**")
        for tk, cx, mpct, unit in makers:
            lines.append(f"- **{tk} {cx}** | unit {fmt(unit)} | %mid {fmt(mpct,2)}%")
        lines.append("")

    return "\n".join(lines)

def render_report(rows: List[SnapshotRow]) -> str:
    lines: List[str] = []
    lines.append(f"# Market report")
    lines.append(f"_Generated at {now_iso()}_")
    lines.append("")
    # overview
    n = len(rows)
    quoted = sum(1 for r in rows if r.mid is not None)
    lines.append(f"- rows: **{n}**")
    lines.append(f"- quoted: **{quoted}**")
    lines.append("")
    # append highlights
    lines.append(render_stats_sections(rows))
    return "\n".join(lines)

# =========================
# watchlists (optional, for per-user copies)
# =========================
_UID_KEYS = ("user_id","uid","user","discord_id","member_id")

def list_user_ids(con: sqlite3.Connection) -> List[str]:
    if not table_exists(con, "user_watchlist"):
        return []
    cols = [r["name"] for r in con.execute("PRAGMA table_info(user_watchlist)").fetchall()]
    low = [c.lower() for c in cols]
    for k in _UID_KEYS:
        if k in low:
            col = cols[low.index(k)]
            ids = [str(r[0]) for r in con.execute(f"SELECT DISTINCT {col} FROM user_watchlist WHERE {col} IS NOT NULL").fetchall()]
            return [i for i in ids if i and i.strip()]
    return []

def load_server_watchlist(con: sqlite3.Connection) -> List[Tuple[str, str]]:
    """
    Returns [(TICKER, CX|''), ...] uppercased. Empty CX means all CXs.
    """
    if not table_exists(con, "server_watchlist"):
        return []
    rows = con.execute("SELECT ticker, COALESCE(exchange,'') FROM server_watchlist").fetchall()
    return [(str(t).upper(), str(x).upper()) for (t, x) in rows]

def load_all_user_watchlists(con: sqlite3.Connection) -> Dict[str, List[Tuple[str, str]]]:
    """
    Returns { user_id: [(TICKER, CX|''), ...] }.
    """
    if not table_exists(con, "user_watchlist"):
        return {}
    out: Dict[str, List[Tuple[str, str]]] = {}
    for uid, tk, ex in con.execute("SELECT user_id, ticker, COALESCE(exchange,'') FROM user_watchlist"):
        key = str(uid)
        out.setdefault(key, []).append((str(tk).upper(), str(ex).upper()))
    return out

def load_user_watchlist(con: sqlite3.Connection, user_id: str) -> List[Tuple[str, str]]:
    if not table_exists(con, "user_watchlist"):
        return []
    rows = con.execute(
        "SELECT ticker, COALESCE(exchange,'') FROM user_watchlist WHERE user_id=?",
        (user_id,)
    ).fetchall()
    return [(str(t).upper(), str(x).upper()) for (t, x) in rows]

def filter_rows_by_watchlist(rows: List[SnapshotRow], wl: List[Tuple[str, str]]) -> List[SnapshotRow]:
    if not wl:
        return []
    want: Dict[str, set] = {}
    for tk, ex in wl:
        want.setdefault(tk, set()).add(ex)  # ex may be '' meaning all CX
    out: List[SnapshotRow] = []
    for r in rows:
        exset = want.get(r.ticker)
        if not exset:
            continue
        if "" in exset or r.cx in exset:
            out.append(r)
    return out

# =========================
# main
# =========================
def update_server_report() -> None:
    ensure_dirs()
    wl: List[Tuple[str, str]] = []
    if os.path.exists(BOT_DB):
        bot_con = open_db(BOT_DB)
        try:
            wl = load_server_watchlist(bot_con)
        finally:
            bot_con.close()
    rows = _compile_universe_rows()
    server_rows = filter_rows_by_watchlist(rows, wl)
    changed = _filter_changed_for_scope(server_rows, scope="server", user_id=None)
    if not changed:
        # no changes → keep previous file to avoid posting noise
        return
    txt = render_watchlist_report(changed)
    with open(REPORT_SERVER_PATH, "w", encoding="utf-8") as f:
        f.write(txt)

def update_user_report(user_id: str) -> None:
    ensure_dirs()
    wl: List[Tuple[str, str]] = []
    if os.path.exists(BOT_DB):
        bot_con = open_db(BOT_DB)
        try:
            wl = load_user_watchlist(bot_con, str(user_id))
        finally:
            bot_con.close()
    rows = _compile_universe_rows()
    u_rows = filter_rows_by_watchlist(rows, wl)
    changed = _filter_changed_for_scope(u_rows, scope="user", user_id=str(user_id))
    if not changed:
        # no changes → keep previous file to avoid posting noise
        return
    txt = render_watchlist_report(changed)
    udir = os.path.join(REPORT_USERS_DIR, str(user_id))
    os.makedirs(udir, exist_ok=True)
    with open(os.path.join(udir, "report.md"), "w", encoding="utf-8") as f:
        f.write(txt)

def main():
    ensure_dirs()

    # load server and user watchlists from bot.db
    server_wl: List[Tuple[str, str]] = []
    user_wls: Dict[str, List[Tuple[str, str]]] = {}
    user_ids: List[str] = []

    if os.path.exists(BOT_DB):
        bot_con = open_db(BOT_DB)
        try:
            server_wl = load_server_watchlist(bot_con)
            user_wls = load_all_user_watchlists(bot_con)
            user_ids = list(user_wls.keys())
        finally:
            bot_con.close()

    prun_con = open_db(PRUN_DB)
    try:
        ensure_snapshot_table(prun_con)
        ensure_market_stats_table(prun_con)

        # Universe = all quotes on all CXs
        uni_quotes = fetch_all_quotes(prun_con, list(CX_ALL))
        uni_rows: List[SnapshotRow] = []
        for q in uni_quotes:
            sr = build_row(prun_con, q)
            if sr:
                insert_snapshot(prun_con, sr)
                uni_rows.append(sr)
        prun_con.commit()

        # Persist stats for universe only
        ts_ms = now_ms()
        persist_stats(prun_con, ts_ms, uni_rows)

        # Universe report (unchanged format)
        uni_report = render_report(uni_rows)
        with open(REPORT_UNI_PATH, "w", encoding="utf-8") as f:
            f.write(uni_report)

        # Server report = watchlist-only + new Discord format
        server_rows = filter_rows_by_watchlist(uni_rows, server_wl)
        server_report = render_watchlist_report(server_rows)
        with open(REPORT_SERVER_PATH, "w", encoding="utf-8") as f:
            f.write(server_report)

        # Per-user reports = each user watchlist-only + new Discord format
        for uid in user_ids:
            udir = os.path.join(REPORT_USERS_DIR, uid)
            os.makedirs(udir, exist_ok=True)
            u_rows = filter_rows_by_watchlist(uni_rows, user_wls.get(uid, []))
            u_report = render_watchlist_report(u_rows)
            with open(os.path.join(udir, "report.md"), "w", encoding="utf-8") as f:
                f.write(u_report)

        # optional legacy
        legacy_path = os.path.join("tmp", "report.md")
        with open(legacy_path, "w", encoding="utf-8") as f:
            f.write(uni_report)

        print(f"Reports written. universe={REPORT_UNI_PATH} server={REPORT_SERVER_PATH} users={len(user_ids)}")

    finally:
        prun_con.close()

if __name__ == "__main__":
    # try:
    #     while True:
    #         t0 = time.time()
            main()
    #         t1 = time.time()
    #         delta = 300 - (t1 - t0)
    #         print(f" ---- finish in {t1 - t0:2f}s - sleep for {delta}s ---- ")
    #         time.sleep(delta)
    # except KeyboardInterrupt:
    #     print(" Exiting Program ")
