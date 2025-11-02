# src/prun-mcp.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PrUn Market MCP Server

A Model Context Protocol (MCP) server that exposes market analysis tools
over HTTP. Provides comprehensive market data analysis, asset management,
and trading tools for Prosperous Universe.

Notes:
- Uses public DB schema: prices(cx,ticker,best_bid,best_ask,PP7,PP30,ts),
  price_history(cx,ticker,bid,ask,ts), materials(ticker,name,category,weight,volume).
- Order-book tools read from books and books_hist if present.
"""

# Standard library imports
import glob
import json
import logging
import math
import os
import signal
import sqlite3
import sys
<<<<<<< HEAD
import json
=======
>>>>>>> 635973c (Implement some small fix with database and add order book related tools. #004)
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional, Tuple

# Third-party imports
from mcp.server.fastmcp import FastMCP

# Local imports
from config import config

# -------- config --------
DB_PATH = config.DB_PATH
PRIVATE_DB = config.PRIVATE_DB
USER_DB = config.USER_DB
STATE_DIR = config.STATE_DIR
TICK = config.TICK
HIST_BUCKET_SECS = config.HIST_BUCKET_SECS

# Reports written by market.py
REPORT_DIR = os.path.join("tmp", "reports")
REPORT_UNIVERSE_FILE = os.path.join(REPORT_DIR, "universe", "report.md")
REPORT_SERVER_FILE   = os.path.join(REPORT_DIR, "server",   "report.md")
REPORT_USERS_DIR     = os.path.join(REPORT_DIR, "users")
USER_JSON_PATH = os.path.join("tmp", "user.json")

CX_REAL = ("NC1","NC2","CI1","CI2","IC1","AI1")
INTERVAL_MAX_DAYS = {"MINUTE_FIVE": 7, "HOUR_TWO": 30}

# Setup logging
log = config.setup_logging("prun-mcp")

mcp = FastMCP(
    "prun-market-mcp",
    instructions="Prosperous Universe market tools for analysis, assets, order planning, history, and order books.",
    stateless_http=True,
)

# -------- DB helpers --------
def db():
    con = sqlite3.connect(DB_PATH, timeout=20.0)
    try:
        con.execute("PRAGMA journal_mode=WAL"); con.execute("PRAGMA synchronous=NORMAL"); con.execute("PRAGMA busy_timeout=20000")
    except Exception:
        pass
    con.row_factory = sqlite3.Row
    return con

def pdb():
    con = sqlite3.connect(PRIVATE_DB, timeout=20.0)
    try:
        con.execute("PRAGMA journal_mode=WAL"); con.execute("PRAGMA synchronous=NORMAL"); con.execute("PRAGMA busy_timeout=20000")
    except Exception:
        pass
    con.row_factory = sqlite3.Row
    return con

def udb():
    con = sqlite3.connect(USER_DB, timeout=20.0)
    try:
        con.execute("PRAGMA journal_mode=WAL"); con.execute("PRAGMA synchronous=NORMAL"); con.execute("PRAGMA busy_timeout=20000")
    except Exception:
        pass
    con.row_factory = sqlite3.Row
    return con

def ts_iso(ts_ms: int) -> str:
    return datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc).isoformat()

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def init_local():
    # Public/local helper tables kept minimal.
    con = db(); cur = con.cursor()
    cur.executescript("""
    PRAGMA journal_mode=WAL;

    CREATE TABLE IF NOT EXISTS assets_holdings(
      id INTEGER PRIMARY KEY,
      ticker TEXT NOT NULL,
      base   TEXT,
      qty    REAL NOT NULL DEFAULT 0,
      avg_cost REAL NOT NULL DEFAULT 0,
      updated_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_assets_holdings_tb ON assets_holdings(ticker,base);

    CREATE TABLE IF NOT EXISTS assets_txn(
      id INTEGER PRIMARY KEY,
      ticker TEXT NOT NULL,
      base   TEXT,
      qty    REAL NOT NULL,
      price  REAL NOT NULL,
      at     TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS planned_orders(
      id INTEGER PRIMARY KEY,
      cx TEXT NOT NULL,
      ticker TEXT NOT NULL,
      side TEXT CHECK(side IN('bid','ask')) NOT NULL,
      price REAL NOT NULL,
      qty REAL NOT NULL,
      filled_qty REAL NOT NULL DEFAULT 0,
      status TEXT NOT NULL DEFAULT 'open',
      created_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_planned_orders ON planned_orders(status,cx,ticker,side);
    """)
    con.commit(); con.close()

    # private DB tables
    pc = pdb(); pcur = pc.cursor()
    pcur.executescript("""
    PRAGMA journal_mode=WAL;

    CREATE TABLE IF NOT EXISTS user_inventory(
      username   TEXT NOT NULL,
      natural_id TEXT,
      name       TEXT,
      storage_type TEXT,
      ticker     TEXT NOT NULL,
      amount     REAL,
      ts INTEGER NOT NULL,
      PRIMARY KEY(username, ticker, natural_id)
    );

    CREATE TABLE IF NOT EXISTS user_cxos(
      username   TEXT NOT NULL,
      order_id   TEXT NOT NULL,
      exchange_code TEXT,
      order_type TEXT,
      material_ticker TEXT,
      amount     REAL,
      initial_amount REAL,
      price_limit REAL,
      currency   TEXT,
      status     TEXT,
      created_epoch_ms INTEGER,
      ts INTEGER NOT NULL,
      PRIMARY KEY(username, order_id)
    );

    CREATE TABLE IF NOT EXISTS user_balances(
      username   TEXT NOT NULL,
      currency   TEXT NOT NULL,
      amount     REAL,
      ts INTEGER NOT NULL,
      PRIMARY KEY(username, currency)
    );
    """)
    pc.commit(); pc.close()

    # users DB
    uc = udb(); ucur = uc.cursor()
    ucur.executescript("""
    PRAGMA journal_mode=WAL;
    CREATE TABLE IF NOT EXISTS users(
      discord_id TEXT PRIMARY KEY,
      fio_api_key TEXT NOT NULL,
      username TEXT,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    );
    """)
    uc.commit(); uc.close()

init_local()

# -------- market utils --------
def best_row(con, cx: str, ticker: str) -> Tuple[Optional[float], Optional[float]]:
    r = con.execute("SELECT best_bid, best_ask FROM prices WHERE cx=? AND ticker=?",
                    (cx, ticker.upper())).fetchone()
    if not r: return (None, None)
    return (float(r["best_bid"]) if r["best_bid"] is not None else None,
            float(r["best_ask"]) if r["best_ask"] is not None else None)

def mid(bb: Optional[float], ba: Optional[float]) -> Optional[float]:
    if bb is None or ba is None: return None
    return round((bb+ba)/2.0, 3)

def _pick_interval(hours: int, explicit: Optional[str]) -> str:
    if explicit in ("MINUTE_FIVE","HOUR_TWO"):
        return explicit
    return "MINUTE_FIVE" if hours <= INTERVAL_MAX_DAYS["MINUTE_FIVE"]*24 else "HOUR_TWO"

def _load_user_json(path: Optional[str] = None) -> Dict[str, Any]:
    p = path or USER_JSON_PATH
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        log.warning("user.json not found at %s", p)
        return {}
    except Exception as e:
        log.exception("Failed to read user.json at %s: %s", p, e)
        return {}
    # Normalize to dict keyed by username if list provided
    if isinstance(data, list):
        normalized = {}
        for i, u in enumerate(data):
            uname = (u or {}).get("UserName") or f"user_{i}"
            normalized[uname] = u
        data = normalized
    if not isinstance(data, dict):
        log.error("user.json not a dict or list. Got %s", type(data))
        return {}
    return data

def _avg(nums: List[Optional[float]]) -> Optional[float]:
    vals = [float(x) for x in nums if x is not None]
    return round(sum(vals) / len(vals), 2) if vals else None


# ====================== MCP TOOLS ======================

# ---- Health ----
@mcp.tool(description="Quick OK + DB paths + time.")
def health() -> Dict[str, Any]:
    m = db(); p = pdb(); u = udb()
    try:
        has_prices = m.execute("SELECT COUNT(*) FROM sqlite_master WHERE name='prices'").fetchone()[0] > 0
        has_hist   = m.execute("SELECT COUNT(*) FROM sqlite_master WHERE name='price_history'").fetchone()[0] > 0
        has_priv_i = p.execute("SELECT COUNT(*) FROM sqlite_master WHERE name='user_inventory'").fetchone()[0] > 0
        has_priv_o = p.execute("SELECT COUNT(*) FROM sqlite_master WHERE name='user_cxos'").fetchone()[0] > 0
        has_priv_b = p.execute("SELECT COUNT(*) FROM sqlite_master WHERE name='user_balances'").fetchone()[0] > 0
        has_users  = u.execute("SELECT COUNT(*) FROM sqlite_master WHERE name='users'").fetchone()[0] > 0
        # order-book presence
        has_books      = m.execute("SELECT COUNT(*) FROM sqlite_master WHERE name='books'").fetchone()[0] > 0
        has_books_hist = m.execute("SELECT COUNT(*) FROM sqlite_master WHERE name='books_hist'").fetchone()[0] > 0
        return {"ok": True, "db_public": DB_PATH, "db_private": PRIVATE_DB, "db_users": USER_DB,
                "has_prices": has_prices, "has_history": has_hist,
                "has_books": has_books, "has_books_hist": has_books_hist,
                "has_private_inventory": has_priv_i, "has_private_cxos": has_priv_o, "has_private_balances": has_priv_b,
                "has_users": has_users, "bucket_ms": HIST_BUCKET_SECS*1000, "time": now_iso()}
    finally:
        m.close(); p.close(); u.close()

# ---- State snapshots ----
@mcp.tool(description="List JSON files written by data.py in STATE_DIR.")
def state_list() -> Dict[str, Any]:
    files = sorted([os.path.basename(p) for p in glob.glob(os.path.join(STATE_DIR, "*.json"))])
    return {"dir": STATE_DIR, "files": files}

@mcp.tool(description="Read a state JSON file by name.")
def state_get(name: str) -> Any:
    path = os.path.join(STATE_DIR, name)
    if not os.path.isfile(path):
        return {"error": f"state file not found: {name}"}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

# ---- Market: best, spread for single ticker ----
@mcp.tool(description="Best bid/ask for a ticker on an exchange.")
def market_best(cx: str, ticker: str) -> Dict[str, Any]:
    con = db()
    try:
        bb, ba = best_row(con, cx, ticker)
        return {"best_bid": bb, "best_ask": ba}
    finally:
        con.close()

@mcp.tool(description="Spread and mid for a ticker on an exchange.")
def market_spread(cx: str, ticker: str) -> Dict[str, Any]:
    con = db()
    try:
        bb, ba = best_row(con, cx, ticker)
        if bb is None or ba is None:
            return {"spread": None, "mid": None, "spread_pct": None}
        s = round(ba - bb, 3); m = mid(bb, ba)
        return {"spread": s, "mid": m, "spread_pct": (round(100.0 * s / m, 3) if m else None)}
    finally:
        con.close()

@mcp.tool(description="Recent traded volume and trade count for cx,ticker over lookback hours.")
def volume_recent(cx: str, ticker: str, hours: int = 48, interval: Optional[str] = None) -> Dict[str, Any]:
    con = db()
    try:
        hours = max(1, int(hours))
        iv = _pick_interval(hours, interval)
        cutoff = int((datetime.now(timezone.utc).timestamp() - hours*3600) * 1000)
        r = con.execute(
            "SELECT COALESCE(SUM(volume),0.0) AS vol, COALESCE(SUM(traded),0) AS trades "
            "FROM prices_chart_history WHERE cx=? AND ticker=? AND interval=? AND ts>=?",
            (cx.upper(), ticker.upper(), iv, cutoff)
        ).fetchone()
        return {
            "cx": cx.upper(), "ticker": ticker.upper(), "hours": hours, "interval": iv,
            "volume": float(r["vol"] or 0.0), "trades": int(r["trades"] or 0)
        }
    finally:
        con.close()

@mcp.tool(description="Per-CX ranking by traded volume for a ticker over lookback hours.")
def cx_volume_ranking(ticker: str, hours: int = 168, interval: Optional[str] = None) -> Dict[str, Any]:
    con = db()
    try:
        iv = _pick_interval(hours, interval)
        cutoff = int((datetime.now(timezone.utc).timestamp() - hours*3600) * 1000)
        rows = con.execute(
            "SELECT cx, COALESCE(SUM(volume),0.0) AS vol, COALESCE(SUM(traded),0) AS trades "
            "FROM prices_chart_history WHERE ticker=? AND interval=? AND ts>=? "
            "AND cx IN ({}) GROUP BY cx".format(",".join("?"*len(CX_REAL))),
            (ticker.upper(), iv, cutoff, *CX_REAL)
        ).fetchall()
        items = [{"cx": r["cx"], "volume": float(r["vol"] or 0.0), "trades": int(r["trades"] or 0)} for r in rows]
        items.sort(key=lambda x: x["volume"], reverse=True)
        return {"ticker": ticker.upper(), "hours": int(hours), "interval": iv, "items": items, "at": now_iso()}
    finally:
        con.close()

def recommend_sell_cx(
    ticker: str,
    hours: int = 72,
    bid_weight: float = 0.7,
    vol_weight: float = 0.3,
    interval: Optional[str] = None
) -> Dict[str, Any]:
    con = db(); con.row_factory = sqlite3.Row
    try:
        t = ticker.upper()
        bids = {r["cx"]: r["best_bid"] for r in con.execute(
            "SELECT cx,best_bid FROM prices WHERE ticker=? AND cx IN ({})".format(",".join("?"*len(CX_REAL))),
            (t, *CX_REAL)
        ).fetchall() if r["best_bid"] is not None}

        iv = _pick_interval(hours, interval)
        cutoff = int((datetime.now(timezone.utc).timestamp() - hours*3600) * 1000)
        vols = {r["cx"]: float(r["v"] or 0.0) for r in con.execute(
            "SELECT cx, SUM(volume) AS v FROM prices_chart_history "
            "WHERE ticker=? AND interval=? AND ts>=? AND cx IN ({}) GROUP BY cx".format(",".join("?"*len(CX_REAL))),
            (t, iv, cutoff, *CX_REAL)
        ).fetchall()}

        # normalize
        def _norm(d: Dict[str, float]) -> Dict[str, float]:
            if not d: return {}
            mn, mx = min(d.values()), max(d.values())
            if mx == mn:
                return {k: 1.0 for k in d}  # flat
            return {k: (v - mn) / (mx - mn) for k, v in d.items()}

        bid_n = _norm(bids)
        # stabilize volume with log to avoid outliers
        vols_log = {k: (0.0 if v <= 0 else math.log1p(v)) for k, v in vols.items()}
        vol_n = _norm(vols_log)

        bw, vw = float(bid_weight), float(vol_weight)
        total_w = (bw + vw) if (bw + vw) > 0 else 1.0
        bw, vw = bw/total_w, vw/total_w

        items = []
        for cx in CX_REAL:
            if cx not in bids: continue
            score = bw * bid_n.get(cx, 0.0) + vw * vol_n.get(cx, 0.0)
            items.append({
                "cx": cx,
                "bid": round(bids.get(cx, 0.0), 3) if cx in bids else None,
                "volume_hours": int(hours),
                "interval": iv,
                "volume": round(vols.get(cx, 0.0), 3),
                "score": round(score, 4)
            })
        items.sort(key=lambda x: x["score"], reverse=True)
        return {"ticker": t, "weights": {"bid": bw, "volume": vw}, "items": items, "at": now_iso()}
    finally:
        con.close()

# ---- Market: spreads across ALL tickers on ONE CX ----
@mcp.tool(description="Spread%% across all tickers on one CX. Sorted by tightest or widest.")
def cx_best_spreads(
    cx: str,
    order: Literal["tightest","widest"] = "tightest",
    limit: int = 200
) -> Dict[str, Any]:
    """
    spread_pct = ((ask - bid) / mid) * 100
    mid = (bid + ask)/2
    """
    con = db(); con.row_factory = sqlite3.Row
    try:
        rows = con.execute(
            "SELECT ticker, best_bid, best_ask FROM prices WHERE cx=?",
            (cx.upper(),)
        ).fetchall()
        out: List[Dict[str, Any]] = []
        for r in rows:
            bb, ba = r["best_bid"], r["best_ask"]
            if bb is None or ba is None:  # need both sides
                continue
            m = (bb + ba) / 2.0
            if m <= 0:
                continue
            spr = ba - bb
            pct = 100.0 * spr / m
            out.append({
                "ticker": r["ticker"].upper(),
                "bid": round(bb, 3),
                "ask": round(ba, 3),
                "mid": round(m, 3),
                "spread": round(spr, 3),
                "spread_pct": round(pct, 3)
            })
        out.sort(key=lambda x: x["spread_pct"], reverse=(order == "widest"))
        return {"cx": cx.upper(), "count": len(out), "items": out[:max(1, int(limit))], "at": now_iso()}
    finally:
        con.close()

# ---- Market: arbitrage pair ----
@mcp.tool(description="Bid-ask edge between two exchanges for one ticker.")
def market_arbitrage_pair(ticker: str, cx_a: str, cx_b: str) -> Dict[str, Any]:
    con = db()
    try:
        a = con.execute("SELECT best_bid,best_ask FROM prices WHERE cx=? AND ticker=?", (cx_a,ticker.upper())).fetchone()
        b = con.execute("SELECT best_bid,best_ask FROM prices WHERE cx=? AND ticker=?", (cx_b,ticker.upper())).fetchone()
        if not a or not b:
            return {"edge_ab": None, "edge_ba": None}
        edge_ab = (a["best_bid"] - b["best_ask"]) if (a["best_bid"] is not None and b["best_ask"] is not None) else None
        edge_ba = (b["best_bid"] - a["best_ask"]) if (b["best_bid"] is not None and a["best_ask"] is not None) else None
        return {"ticker": ticker.upper(), "buy_at": cx_b, "sell_at": cx_a,
                "edge_ab": (round(edge_ab,3) if edge_ab is not None else None),
                "buy_at_rev": cx_a, "sell_at_rev": cx_b,
                "edge_ba": (round(edge_ba,3) if edge_ba is not None else None)}
    finally:
        con.close()

# ---- Local Market: arbitrage vs CX (minimal) ----
@mcp.tool(description="LM sells below CX ask or LM buys above CX bid. Minimal fields.")
def lm_arbitrage_near_cx(
    cx: str,
    planet_ids_csv: Optional[str] = None,
    min_edge_pct: float = 0.5,
    limit: int = 50,
    tickers_csv: Optional[str] = None,
    currency: Optional[str] = None
) -> Dict[str, Any]:
    con = db(); con.row_factory = sqlite3.Row
    try:
        cxmap = {
            r["ticker"].upper(): (r["best_bid"], r["best_ask"])
            for r in con.execute("SELECT ticker,best_bid,best_ask FROM prices WHERE cx=?", (cx,))
        }

        params_sell: List[Any] = []; params_buy: List[Any] = []
        where_parts = ["Price IS NOT NULL", "MaterialTicker IS NOT NULL"]

        if planet_ids_csv:
            pids = [p.strip() for p in planet_ids_csv.split(",") if p.strip()]
            if pids:
                marks = ",".join("?" for _ in pids)
                where_parts.append(f"PlanetNaturalId IN ({marks})")
                params_sell += pids; params_buy += pids
        if tickers_csv:
            tcks = [t.strip().upper() for t in tickers_csv.split(",") if t.strip()]
            if tcks:
                marks = ",".join("?" for _ in tcks)
                where_parts.append(f"UPPER(MaterialTicker) IN ({marks})")
                params_sell += tcks; params_buy += tcks
        if currency:
            where_parts.append("PriceCurrency = ?")
            params_sell.append(currency); params_buy.append(currency)

        where_sql = " AND ".join(where_parts)

        sell_rows = con.execute(f"""
            SELECT PlanetNaturalId,PlanetName,MaterialTicker,MaterialAmount,Price,PriceCurrency
            FROM LM_sell WHERE {where_sql}
        """, params_sell).fetchall()

        buy_rows = con.execute(f"""
            SELECT PlanetNaturalId,PlanetName,MaterialTicker,MaterialAmount,Price,PriceCurrency
            FROM LM_buy WHERE {where_sql}
        """, params_buy).fetchall()

        sell_edges: List[Dict[str, Any]] = []
        for r in sell_rows:
            t = (r["MaterialTicker"] or "").upper()
            price = r["Price"]; bb, ba = cxmap.get(t, (None, None))
            if ba is None or price is None or price >= ba: continue
            edge_pct = round(100.0 * (ba - price) / ba, 3)
            amt = float(r["MaterialAmount"] or 0) or 0.0
            ppu = (price / amt) if (price is not None and amt > 0) else None
            if edge_pct >= min_edge_pct:
                sell_edges.append({
                    "ticker": t,
                    "planet_id": r["PlanetNaturalId"],
                    "planet": r["PlanetName"],
                    "lm_price": price,
                    "lm_ppu": (round(ppu,3) if ppu is not None else None),
                    "cx_ask": ba,
                    "edge_pct": edge_pct,
                    "amount": r["MaterialAmount"],
                    "ccy": r["PriceCurrency"],
                })

        buy_edges: List[Dict[str, Any]] = []
        for r in buy_rows:
            t = (r["MaterialTicker"] or "").upper()
            price = r["Price"]; bb, ba = cxmap.get(t, (None, None))
            if bb is None or price is None or price <= bb: continue
            edge_pct = round(100.0 * (price - bb) / bb, 3)
            amt = float(r["MaterialAmount"] or 0) or 0.0
            ppu = (price / amt) if (price is not None and amt > 0) else None
            if edge_pct >= min_edge_pct:
                buy_edges.append({
                    "ticker": t,
                    "planet_id": r["PlanetNaturalId"],
                    "planet": r["PlanetName"],
                    "lm_price": price,
                    "lm_ppu": (round(ppu,3) if ppu is not None else None),
                    "cx_bid": bb,
                    "edge_pct": edge_pct,
                    "amount": r["MaterialAmount"],
                    "ccy": r["PriceCurrency"],
                })

        sell_edges.sort(key=lambda x: (-x["edge_pct"], x["ticker"], x["planet_id"]))
        buy_edges.sort(key=lambda x: (-x["edge_pct"], x["ticker"], x["planet_id"]))

        return {"cx": cx, "min_edge_pct": min_edge_pct,
                "sell": sell_edges[:max(1, int(limit))],
                "buy":  buy_edges[:max(1, int(limit))],
                "at": now_iso()}
    finally:
        con.close()

# ---- Local Market: simple search (minimal) ----
@mcp.tool(description="Search LM buy/sell. Minimal fields. side in {'buy','sell','both'}.")
def lm_search(
    q: str,
    side: Literal["buy","sell","both"] = "both",
    planet_ids_csv: Optional[str] = None,
    currency: Optional[str] = None,
    limit: int = 100
) -> Dict[str, Any]:
    con = db(); con.row_factory = sqlite3.Row
    try:
        like = f"%{q.strip()}%" if q else "%"
        where_parts = ["Price IS NOT NULL",
                       "(UPPER(MaterialTicker) LIKE ? OR UPPER(MaterialName) LIKE ?)"]
        params: List[Any] = [like.upper(), like.upper()]

        if planet_ids_csv:
            pids = [p.strip() for p in planet_ids_csv.split(",") if p.strip()]
            if pids:
                marks = ",".join("?" for _ in pids)
                where_parts.append(f"PlanetNaturalId IN ({marks})")
                params += pids
        if currency:
            where_parts.append("PriceCurrency = ?")
            params.append(currency)

        where_sql = " AND ".join(where_parts)
        lim = max(1, int(limit))

        def _run(table: str) -> List[Dict[str, Any]]:
            rows = con.execute(f"""
                SELECT PlanetNaturalId,PlanetName,MaterialTicker,MaterialAmount,Price,PriceCurrency
                FROM {table} WHERE {where_sql}
                ORDER BY UPPER(MaterialTicker), PlanetNaturalId
                LIMIT ?
            """, params + [lim]).fetchall()
            out = []
            for r in rows:
                amt = float(r["MaterialAmount"] or 0) or 0.0
                price = float(r["Price"] or 0) if r["Price"] is not None else None
                ppu = (price / amt) if (price is not None and amt > 0) else None
                out.append({
                    "ticker": r["MaterialTicker"].upper(),
                    "planet_id": r["PlanetNaturalId"],
                    "planet": r["PlanetName"],
                    "price": price,
                    "amount": r["MaterialAmount"],
                    "ppu": (round(ppu, 3) if ppu is not None else None),
                    "ccy": r["PriceCurrency"],
                })
            return out

        out: Dict[str, Any] = {"at": now_iso()}
        if side in ("sell","both"): out["sell"] = _run("LM_sell")
        if side in ("buy","both"):  out["buy"]  = _run("LM_buy")
        return out
    finally:
        con.close()

# ---- Materials ----
@mcp.tool(description="Get material info by ticker.")
def materials_name(ticker: str) -> Dict[str, Any]:
    con = db()
    try:
        r = con.execute("SELECT ticker,name,category,weight,volume FROM materials WHERE ticker=?",
                        (ticker.upper(),)).fetchone()
        return dict(r) if r else {"error": "ticker not found"}
    finally:
        con.close()

@mcp.tool(description="Search materials by ticker or name prefix.")
def materials_lookup(prefix: str) -> Dict[str, Any]:
    p = f"%{prefix.upper()}%"
    con = db()
    try:
        rows = con.execute(
            "SELECT ticker,name,category,weight,volume FROM materials "
            "WHERE UPPER(ticker) LIKE ? OR UPPER(name) LIKE ? ORDER BY ticker LIMIT 50",
            (p,p)
        ).fetchall()
        return {"items": [dict(r) for r in rows]}
    finally:
        con.close()

# ---- Assets (local) ----
@mcp.tool(description="List local holdings.")
def assets_holdings() -> Dict[str, Any]:
    con = db()
    try:
        rows = con.execute("SELECT id,ticker,base,qty,avg_cost,updated_at FROM assets_holdings ORDER BY ticker, base").fetchall()
        return {"items": [dict(r) for r in rows]}
    finally:
        con.close()

@mcp.tool(description="Record a transaction. qty>0 buy, qty<0 sell.")
def assets_txn(ticker: str, qty: float, price: float, base: Optional[str]=None) -> Dict[str, Any]:
    if qty == 0:
        return {"error": "qty must be non-zero"}
    con = db(); cur = con.cursor()
    try:
        tkr = ticker.upper()
        cur.execute("INSERT INTO assets_txn(ticker,base,qty,price,at) VALUES(?,?,?,?,?)", (tkr, base, qty, price, now_iso()))
        r = cur.execute("SELECT id,qty,avg_cost FROM assets_holdings WHERE ticker=? AND ifnull(base,'')=ifnull(?, '')",
                        (tkr, base)).fetchone()
        if not r:
            cur.execute("INSERT INTO assets_holdings(ticker,base,qty,avg_cost,updated_at) VALUES(?,?,?,?,?)", (tkr, base, qty, price, now_iso()))
        else:
            hid, hq, hc = r["id"], float(r["qty"]), float(r["avg_cost"])
            new_qty = hq + qty
            if new_qty < -1e-9:
                con.rollback()
                return {"error": f"would make negative holding: {hq} + {qty}"}
            new_cost = ((hq*hc) + (qty*price)) / (new_qty if new_qty>0 else max(qty,1)) if qty>0 else (hc if new_qty>0 else 0.0)
            cur.execute("UPDATE assets_holdings SET qty=?, avg_cost=?, updated_at=? WHERE id=?", (new_qty, new_cost, now_iso(), hid))
        con.commit()
        return {"ok": True}
    finally:
        con.close()

@mcp.tool(description="Mark holdings to market on an exchange.")
def assets_value(cx: str, method: Literal["bid","ask","mid"]="mid", base: Optional[str]=None) -> Dict[str, Any]:
    con = db()
    try:
        where = "WHERE 1=1"; params=[]
        if base: where += " AND base=?"; params.append(base)
        rows = con.execute(f"SELECT ticker, base, qty, avg_cost FROM assets_holdings {where}", params).fetchall()
        total=0.0; items=[]
        for r in rows:
            px = con.execute("SELECT best_bid,best_ask FROM prices WHERE cx=? AND ticker=?", (cx, r["ticker"])).fetchone()
            if not px: price=None
            else:
                bb,ba = px["best_bid"], px["best_ask"]
                price = {"bid": bb, "ask": ba, "mid": (None if (bb is None or ba is None) else round((bb+ba)/2.0,3))}[method]
            val = (r["qty"]*price) if (price is not None) else None
            if val is not None: total += val
            items.append({"ticker": r["ticker"], "base": r["base"], "qty": r["qty"], "price": price, "value": (round(val,3) if val is not None else None)})
        return {"cx": cx, "method": method, "total_value": round(total,3), "items": items}
    finally:
        con.close()

# ---- History (price_history) ----
def _row_to_hist_dict(r: sqlite3.Row) -> Dict[str, Any]:
    bb = r["bid"]; ba = r["ask"]
    mid_v = (None if (bb is None or ba is None) else round((bb+ba)/2.0, 3))
    spr_v = (None if (bb is None or ba is None) else round(ba-bb, 3))
    return {
        "ts": int(r["ts"]),
        "time": ts_iso(r["ts"]),
        "best_bid": bb,
        "best_ask": ba,
        "mid": mid_v,
        "spread": spr_v,
    }

@mcp.tool(description="Latest N history rows for cx,ticker. Returns ascending by ts.")
def history_latest(cx: str, ticker: str, n: int = 288) -> Dict[str, Any]:
    con = db()
    try:
        rows = con.execute(
            "SELECT ts,bid,ask FROM price_history WHERE cx=? AND ticker=? ORDER BY ts DESC LIMIT ?",
            (cx, ticker.upper(), int(n))
        ).fetchall()
        items = [_row_to_hist_dict(r) for r in reversed(rows)]
        return {"cx": cx, "ticker": ticker.upper(), "bucket_ms": HIST_BUCKET_SECS*1000, "rows": items}
    finally:
        con.close()

@mcp.tool(description="History since N minutes ago for cx,ticker. Returns ascending by ts.")
def history_since(cx: str, ticker: str, minutes: int = 60) -> Dict[str, Any]:
    con = db()
    try:
        cutoff_ms = int((datetime.now(timezone.utc).timestamp() - minutes*60) * 1000)
        rows = con.execute(
            "SELECT ts,bid,ask FROM price_history WHERE cx=? AND ticker=? AND ts>=? ORDER BY ts ASC",
            (cx, ticker.upper(), cutoff_ms)
        ).fetchall()
        items = [_row_to_hist_dict(r) for r in rows]
        return {"cx": cx, "ticker": ticker.upper(), "since_ms": cutoff_ms, "bucket_ms": HIST_BUCKET_SECS*1000, "rows": items}
    finally:
        con.close()

@mcp.tool(description="History between [start_ts_ms,end_ts_ms] for cx,ticker. Returns ascending by ts.")
def history_between(cx: str, ticker: str, start_ts: int, end_ts: int, limit: int = 10000) -> Dict[str, Any]:
    if end_ts < start_ts:
        return {"error": "end_ts < start_ts"}
    con = db()
    try:
        rows = con.execute(
            "SELECT ts,bid,ask FROM price_history WHERE cx=? AND ticker=? AND ts BETWEEN ? AND ? ORDER BY ts ASC LIMIT ?",
            (cx, ticker.upper(), int(start_ts), int(end_ts), int(limit))
        ).fetchall()
        items = [_row_to_hist_dict(r) for r in rows]
        return {"cx": cx, "ticker": ticker.upper(), "start_ts": int(start_ts), "end_ts": int(end_ts),
                "bucket_ms": HIST_BUCKET_SECS*1000, "rows": items}
    finally:
        con.close()

@mcp.tool(description="Basic stats over history since N minutes: min/max/avg for bid, ask, spread.")
def history_stats(cx: str, ticker: str, minutes: int = 1440) -> Dict[str, Any]:
    con = db()
    try:
        cutoff_ms = int((datetime.now(timezone.utc).timestamp() - minutes*60) * 1000)
        rows = con.execute(
            "SELECT bid,ask FROM price_history WHERE cx=? AND ticker=? AND ts>=? ORDER BY ts ASC",
            (cx, ticker.upper(), cutoff_ms)
        ).fetchall()
        if not rows:
            return {"cx": cx, "ticker": ticker.upper(), "count": 0}
        bids = [r["bid"] for r in rows if r["bid"] is not None]
        asks = [r["ask"] for r in rows if r["ask"] is not None]
        spreads = [(r["ask"] - r["bid"]) for r in rows if (r["bid"] is not None and r["ask"] is not None)]
        def _agg(arr):
            if not arr: return {"min": None, "max": None, "avg": None}
            return {"min": round(min(arr),3), "max": round(max(arr),3), "avg": round(sum(arr)/len(arr),3)}
        return {"cx": cx, "ticker": ticker.upper(), "count": len(rows), "since_ms": cutoff_ms,
                "bid": _agg(bids), "ask": _agg(asks), "spread": _agg(spreads)}
    finally:
        con.close()

# ---- Reports (read pre-rendered markdown from tmp/reports) ----
def _read_text(path: str) -> Optional[str]:
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return f.read().strip()
    except Exception as e:
        log.error(f"read report error {path}: {e}")
    return None

@mcp.tool(description="Read rendered market report markdown. kind in {'universe','server','user'}. For 'user', pass user_id.")
def market_report(kind: Literal["universe","server","user"]="server", user_id: Optional[str]=None) -> Dict[str, Any]:
    if kind == "universe":
        txt = _read_text(REPORT_UNIVERSE_FILE)
        return {"kind": "universe", "path": REPORT_UNIVERSE_FILE, "exists": bool(txt), "content": txt}
    if kind == "server":
        txt = _read_text(REPORT_SERVER_FILE)
        return {"kind": "server", "path": REPORT_SERVER_FILE, "exists": bool(txt), "content": txt}
    if kind == "user":
        if not user_id:
            return {"error": "user_id required for kind='user'"}
        p = os.path.join(REPORT_USERS_DIR, str(user_id), "report.md")
        txt = _read_text(p)
        return {"kind": "user", "user_id": str(user_id), "path": p, "exists": bool(txt), "content": txt}
    return {"error": "invalid kind"}

# ---- Private user data ----
@mcp.tool(description="Show masked FNAR CSV key and username for a Discord user.")
def user_key_info(discord_id: str) -> Dict[str, Any]:
    con = udb()
    try:
        r = con.execute("SELECT fio_api_key, username, created_at, updated_at FROM users WHERE discord_id=?", (discord_id,)).fetchone()
        if not r:
            return {"discord_id": discord_id, "exists": False}
        k = r["fio_api_key"]; masked = (k[:4] + "*"*max(0,len(k)-8) + k[-4:]) if k else None
        return {"discord_id": discord_id, "exists": True, "username": r["username"], "key_masked": masked,
                "created_at": r["created_at"], "updated_at": r["updated_at"]}
    finally:
        con.close()

@mcp.tool(description="User inventory by FIO username. Optional filters.")
def user_inventory(username: str, ticker: Optional[str]=None, storage_type: Optional[str]=None,
                   name_like: Optional[str]=None, min_amount: float = 0.0, limit: int = 500) -> Dict[str, Any]:
    con = pdb()
    try:
        where = ["username=?"]; args=[username]
        if ticker: where.append("ticker=?"); args.append(ticker.upper())
        if storage_type: where.append("storage_type=?"); args.append(storage_type)
        if name_like: where.append("UPPER(name) LIKE ?"); args.append(f"%{name_like.upper()}%")
        if min_amount > 0: where.append("amount>=?"); args.append(min_amount)
        sql = "SELECT username,natural_id,name,storage_type,ticker,amount,ts FROM user_inventory WHERE " + " AND ".join(where) + " ORDER BY ticker,name LIMIT ?"
        args.append(int(limit))
        rows = con.execute(sql, args).fetchall()
        return {"username": username, "rows": [dict(r) for r in rows]}
    finally:
        con.close()

@mcp.tool(description="User CXOS orders by FIO username.")
def user_cxos(username: str, status: Optional[str]=None, exchange_code: Optional[str]=None,
              ticker: Optional[str]=None, limit: int = 500) -> Dict[str, Any]:
    con = pdb()
    try:
        where = ["username=?"]; args=[username]
        if status: where.append("status=?"); args.append(status.upper())
        if exchange_code: where.append("exchange_code=?"); args.append(exchange_code.upper())
        if ticker: where.append("material_ticker=?"); args.append(ticker.upper())
        sql = ("SELECT username,order_id,exchange_code,order_type,material_ticker,amount,initial_amount,price_limit,"
               "currency,status,created_epoch_ms,ts FROM user_cxos WHERE " + " AND ".join(where) +
               " ORDER BY created_epoch_ms DESC LIMIT ?")
        args.append(int(limit))
        rows = con.execute(sql, args).fetchall()
        return {"username": username, "rows": [dict(r) for r in rows]}
    finally:
        con.close()

@mcp.tool(description="User balances by currency for FIO username.")
def user_balances(username: str) -> Dict[str, Any]:
    con = pdb()
    try:
        rows = con.execute("SELECT username,currency,amount,ts FROM user_balances WHERE username=? ORDER BY currency", (username,)).fetchall()
        return {"username": username, "rows": [dict(r) for r in rows]}
    finally:
        con.close()

@mcp.tool(description="Count users grouped by CountryCode from ./tmp/user.json. Optional country_code or username filter.")
def faction_player_counts(country_code: Optional[str] = None, username: Optional[str] = None, limit: int = 0) -> Dict[str, Any]:
    users = _load_user_json()
    if not users:
        return {"path": USER_JSON_PATH, "exists": False, "rows": []}
    counts: Dict[str, int] = {}
    cnames: Dict[str, str] = {}
    for u in users.values():
        if not isinstance(u, dict):
            continue
        cc = (u.get("CountryCode") or "UNK").upper()
        cn = u.get("CountryName") or "Unknown"
        counts[cc] = counts.get(cc, 0) + 1
        cnames[cc] = cn

    # Derive from username if provided and no country_code
    if not country_code and username:
        u = users.get(username)
        if isinstance(u, dict):
            country_code = (u.get("CountryCode") or "").upper() or None

    if country_code:
        cc = country_code.upper()
        return {
            "path": USER_JSON_PATH,
            "exists": True,
            "country_code": cc,
            "country_name": cnames.get(cc),
            "count": counts.get(cc, 0),
        }

    rows = [{"CountryCode": cc, "CountryName": cnames.get(cc), "Count": c} for cc, c in counts.items()]
    rows.sort(key=lambda r: r["Count"], reverse=True)
    if limit and limit > 0:
        rows = rows[:limit]
    return {"path": USER_JSON_PATH, "exists": True, "total_users": sum(counts.values()), "rows": rows}

@mcp.tool(description="Search users by username, company name, or company code in ./tmp/user.json and return key profile fields.")
def users_info_search(query: str, limit: int = 20) -> Dict[str, Any]:
    q = (query or "").strip().lower()
    if not q:
        return {"error": "query required"}
    users = _load_user_json()
    if not users:
        return {"path": USER_JSON_PATH, "exists": False, "rows": []}

    matches = []
    now_ms = int(time.time() * 1000)
    for uname, u in users.items():
        if not isinstance(u, dict):
            continue
        fields = [
            uname or "",
            str(u.get("CompanyName") or ""),
            str(u.get("CompanyCode") or ""),
        ]
        if not any(q in f.lower() for f in fields):
            continue

        created_ms = u.get("CreatedEpochMs")
        age_days = int((now_ms - int(created_ms)) / 86400000) if isinstance(created_ms, (int, float)) else None
        planets = [
            {"name": p.get("PlanetName"), "nid": p.get("PlanetNaturalId")}
            for p in (u.get("Planets") or [])
            if isinstance(p, dict)
        ]
        activity = u.get("ActivityRating")
        reliability = u.get("ReliabilityRating")
        stability = u.get("StabilityRating")
        overall = _avg([activity, reliability, stability])

        row = {
            "UserName": u.get("UserName") or uname,
            "CompanyName": u.get("CompanyName"),
            "CompanyCode": u.get("CompanyCode"),
            "SubscriptionLevel": u.get("SubscriptionLevel"),
            "Planets": planets,
            "Ratings": {
                "activity": activity,
                "reliability": reliability,
                "stability": stability,
                "overall": overall,
            },
            "Corporation": {
                "name": u.get("CorporationName"),
                "code": u.get("CorporationCode"),
            },
            "HeadquartersLevel": u.get("HeadquartersLevel", -1),
            "CompanyAgeDays": age_days,
            "CountryCode": u.get("CountryCode"),
            "CountryName": u.get("CountryName"),
        }
        matches.append(row)
        if len(matches) >= limit:
            break

    return {"path": USER_JSON_PATH, "exists": True, "count": len(matches), "rows": matches}

<<<<<<< HEAD
=======
# ====================== ORDER BOOK TOOLS ======================

def _now_ms() -> int:
    return int(time.time() * 1000)

def _latest_ts_for_side(c: sqlite3.Connection, cx: str, ticker: str, side: str) -> Optional[int]:
    cur = c.execute(
        "SELECT MAX(ts) AS ts FROM books_hist WHERE cx=? AND ticker=? AND side=?",
        (cx.upper(), ticker.upper(), side)
    ).fetchone()
    if cur and cur["ts"]:
        return int(cur["ts"])
    cur = c.execute(
        "SELECT MAX(ts) AS ts FROM books WHERE cx=? AND ticker=? AND side=?",
        (cx.upper(), ticker.upper(), side)
    ).fetchone()
    return int(cur["ts"]) if cur and cur["ts"] else None

def _levels_aggregated(
    c: sqlite3.Connection,
    cx: str,
    ticker: str,
    side: Literal["bid","ask"],
    depth: int
) -> Tuple[Optional[int], List[Dict[str, Any]]]:
    """
    Return latest timestamp and top-N aggregated price levels for a side.
    Aggregation: sum qty per price at the latest snapshot ts.
    Ordering: bids desc, asks asc. Levels start at 1.
    """
    ts = _latest_ts_for_side(c, cx, ticker, side)
    if ts is None:
        return (None, [])
    order = "DESC" if side == "bid" else "ASC"

    rows = c.execute(
        f"""
        SELECT price, SUM(qty) AS qty
        FROM books_hist
        WHERE cx=? AND ticker=? AND side=? AND ts=?
        GROUP BY price
        ORDER BY price {order}
        LIMIT ?
        """,
        (cx.upper(), ticker.upper(), side, ts, depth)
    ).fetchall()
    if not rows:
        rows = c.execute(
            f"""
            SELECT price, SUM(qty) AS qty
            FROM books
            WHERE cx=? AND ticker=? AND side=?
            GROUP BY price
            ORDER BY price {order}
            LIMIT ?
            """,
            (cx.upper(), ticker.upper(), side, depth)
        ).fetchall()

    out: List[Dict[str, Any]] = []
    for idx, r in enumerate(rows, start=1):
        out.append({
            "level": idx,
            "price": float(r["price"]),
            "qty": float(r["qty"]),
        })
    return (ts, out)

def _best_price_and_size(
    c: sqlite3.Connection, cx: str, ticker: str, side: Literal["bid","ask"]
) -> Tuple[Optional[int], Optional[float], float]:
    """
    Best price and total size at that price from latest snapshot.
    Returns (ts, price, size). price None if side empty.
    """
    ts = _latest_ts_for_side(c, cx, ticker, side)
    if ts is None:
        return (None, None, 0.0)

    if side == "bid":
        pr = c.execute(
            "SELECT MAX(price) AS p FROM books_hist WHERE cx=? AND ticker=? AND side=? AND ts=?",
            (cx.upper(), ticker.upper(), side, ts)
        ).fetchone()
    else:
        pr = c.execute(
            "SELECT MIN(price) AS p FROM books_hist WHERE cx=? AND ticker=? AND side=? AND ts=?",
            (cx.upper(), ticker.upper(), side, ts)
        ).fetchone()

    price = float(pr["p"]) if pr and pr["p"] is not None else None
    if price is None:
        # Fallback to books
        if side == "bid":
            pr = c.execute(
                "SELECT MAX(price) AS p FROM books WHERE cx=? AND ticker=? AND side=?",
                (cx.upper(), ticker.upper(), side)
            ).fetchone()
        else:
            pr = c.execute(
                "SELECT MIN(price) AS p FROM books WHERE cx=? AND ticker=? AND side=?",
                (cx.upper(), ticker.upper(), side)
            ).fetchone()
        price = float(pr["p"]) if pr and pr["p"] is not None else None
        if price is None:
            return (ts, None, 0.0)
        q = c.execute(
            "SELECT SUM(qty) AS sz FROM books WHERE cx=? AND ticker=? AND side=? AND price=?",
            (cx.upper(), ticker.upper(), side, price)
        ).fetchone()
        return (ts, price, float(q["sz"] or 0.0))

    q = c.execute(
        "SELECT SUM(qty) AS sz FROM books_hist WHERE cx=? AND ticker=? AND side=? AND ts=? AND price=?",
        (cx.upper(), ticker.upper(), side, ts, price)
    ).fetchone()
    return (ts, price, float(q["sz"] or 0.0))

def _snapshot_rows(c: sqlite3.Connection, cx: str, ticker: str, side: Literal["bid","ask"]) -> Tuple[Optional[int], List[Tuple[float, float]]]:
    ts = _latest_ts_for_side(c, cx, ticker, side)
    if ts is None:
        return (None, [])
    rows = c.execute(
        "SELECT price, qty FROM books_hist WHERE cx=? AND ticker=? AND side=? AND ts=?",
        (cx.upper(), ticker.upper(), side, ts)
    ).fetchall()
    if not rows:
        rows = c.execute(
            "SELECT price, qty FROM books WHERE cx=? AND ticker=? AND side=?",
            (cx.upper(), ticker.upper(), side)
        ).fetchall()
    return (ts, [(float(r["price"]), float(r["qty"])) for r in rows])

def _cluster_bins(rows: List[Tuple[float, float]], bin_size: float) -> List[Dict[str, float]]:
    """
    rows: list of (price, qty) at latest snapshot for one side.
    bin_size: cluster width in credits. Must be > 0.
    Returns sorted clusters by total qty desc. Each cluster has:
      {"qty": total_qty, "price": weighted_avg_price, "bin_lo": lo, "bin_hi": hi}
    """
    if not rows or bin_size <= 0:
        return []
    bins: Dict[int, Dict[str, float]] = {}
    for price, qty in rows:
        key = int(math.floor(price / bin_size))
        lo = key * bin_size
        hi = lo + bin_size
        b = bins.setdefault(key, {"qty": 0.0, "wpx": 0.0, "lo": lo, "hi": hi})
        b["qty"] += qty
        b["wpx"] += price * qty
    clusters: List[Dict[str, float]] = []
    for b in bins.values():
        price = (b["wpx"] / b["qty"]) if b["qty"] > 0 else (b["lo"] + b["hi"]) / 2.0
        clusters.append({"qty": b["qty"], "price": price, "bin_lo": b["lo"], "bin_hi": b["hi"]})
    clusters.sort(key=lambda x: (-x["qty"], x["price"]))
    return clusters

# --------- Public Order-Book Tools ---------

@mcp.tool(description="Order book levels. Top-N price levels per side at latest snapshot. Aggregated by price.")
def ob_levels(cx: str, ticker: str, depth: int = 20) -> Dict[str, Any]:
    cx = cx.upper().strip()
    ticker = ticker.upper().strip()
    depth = max(1, int(depth))

    with db() as c:
        ts_b, bids = _levels_aggregated(c, cx, ticker, "bid", depth)
        ts_a, asks = _levels_aggregated(c, cx, ticker, "ask", depth)
    ts = max([t for t in [ts_b, ts_a] if t is not None], default=None)

    return {
        "cx": cx,
        "ticker": ticker,
        "ts": ts,
        "bids": bids,
        "asks": asks,
    }

@mcp.tool(description="Order book imbalance over top-N price levels at latest snapshot. Returns imbalance in [-1,1].")
def ob_imbalance(cx: str, ticker: str, depth: int = 10) -> Dict[str, Any]:
    cx = cx.upper().strip()
    ticker = ticker.upper().strip()
    depth = max(1, int(depth))

    with db() as c:
        ts_b, bids = _levels_aggregated(c, cx, ticker, "bid", depth)
        ts_a, asks = _levels_aggregated(c, cx, ticker, "ask", depth)

    bvol = float(sum(l["qty"] for l in bids))
    avol = float(sum(l["qty"] for l in asks))
    denom = bvol + avol
    imb = None if denom == 0 else (bvol - avol) / denom
    ts = max([t for t in [ts_b, ts_a] if t is not None], default=None)

    return {
        "cx": cx,
        "ticker": ticker,
        "ts": ts,
        "depth": depth,
        "bid_vol": bvol,
        "ask_vol": avol,
        "imbalance": imb,
    }

@mcp.tool(description="Microprice using aggregated size at best bid/ask. Returns bests and microprice.")
def ob_microprice(cx: str, ticker: str) -> Dict[str, Any]:
    cx = cx.upper().strip()
    ticker = ticker.upper().strip()
    with db() as c:
        ts_b, bb, bsz = _best_price_and_size(c, cx, ticker, "bid")
        ts_a, ba, asz = _best_price_and_size(c, cx, ticker, "ask")
    ts = max([t for t in [ts_b, ts_a] if t is not None], default=None)

    micro = None
    if bb is not None and ba is not None and (bsz + asz) > 0:
        micro = (ba * bsz + bb * asz) / (bsz + asz)

    return {
        "cx": cx,
        "ticker": ticker,
        "ts": ts,
        "best_bid": bb,
        "best_ask": ba,
        "bid_size": bsz,
        "ask_size": asz,
        "microprice": micro,
    }

@mcp.tool(description="Support/Resistance from order book clusters or recent price history.")
def ob_support_resistance(
    cx: str,
    ticker: str,
    mode: Literal["book","history"] = "book",
    cluster: float = 1.0,
    top: int = 3,
    lookback_h: int = 168
) -> Dict[str, Any]:
    cx = cx.upper().strip()
    ticker = ticker.upper().strip()
    top = max(1, int(top))

    if mode == "book":
        cluster = float(cluster if cluster and cluster > 0 else 1.0)
        with db() as c:
            ts_b, bid_rows = _snapshot_rows(c, cx, ticker, "bid")
            ts_a, ask_rows = _snapshot_rows(c, cx, ticker, "ask")
        ts = max([t for t in [ts_b, ts_a] if t is not None], default=None)

        bid_clusters = _cluster_bins(bid_rows, cluster)[:top]
        ask_clusters = _cluster_bins(ask_rows, cluster)[:top]

        supports = [{"price": round(x["price"], 2), "size": x["qty"], "bin": [x["bin_lo"], x["bin_hi"]]} for x in bid_clusters]
        resistances = [{"price": round(x["price"], 2), "size": x["qty"], "bin": [x["bin_lo"], x["bin_hi"]]} for x in ask_clusters]

        return {
            "cx": cx, "ticker": ticker, "ts": ts,
            "mode": "book", "cluster": cluster, "top": top,
            "supports": supports, "resistances": resistances
        }

    # mode == 'history'
    now_ms = _now_ms()
    look_ms = int(lookback_h) * 3600_000
    start = now_ms - look_ms

    with db() as c:
        rows = c.execute(
            """
            SELECT ts, bid, ask
            FROM price_history
            WHERE cx=? AND ticker=? AND ts>=?
            ORDER BY ts ASC
            """,
            (cx, ticker, start)
        ).fetchall()

    if not rows:
        return {
            "cx": cx, "ticker": ticker, "ts": None,
            "mode": "history", "lookback_h": lookback_h,
            "supports": [], "resistances": []
        }

    # Build mid prices when both sides exist, fallback to available side
    series: List[Tuple[int, float]] = []
    for r in rows:
        b = r["bid"]; a = r["ask"]
        price = None
        if b is not None and a is not None:
            price = (float(b) + float(a)) / 2.0
        elif b is not None:
            price = float(b)
        elif a is not None:
            price = float(a)
        if price is not None:
            series.append((int(r["ts"]), price))

    # Find swing highs/lows using a small neighborhood
    k = 3
    highs: List[Tuple[float, int]] = []
    lows: List[Tuple[float, int]] = []
    for i in range(k, len(series) - k):
        p = series[i][1]
        left = [series[j][1] for j in range(i - k, i)]
        right = [series[j][1] for j in range(i + 1, i + 1 + k)]
        if all(p >= x for x in left + right):
            highs.append((p, series[i][0]))
        if all(p <= x for x in left + right):
            lows.append((p, series[i][0]))

    # Cluster highs and lows into bands
    def cluster_points(points: List[Tuple[float, int]], bands: int = 50) -> List[Dict[str, float]]:
        if not points:
            return []
        ps = [p for p, _ in points]
        pmin, pmax = min(ps), max(ps)
        if pmax == pmin:
            return [{"price": pmin, "size": float(len(points))}]
        width = max(1e-9, (pmax - pmin) / bands)
        buckets: Dict[int, Dict[str, float]] = {}
        for price, _ts in points:
            key = int(math.floor((price - pmin) / width))
            b = buckets.setdefault(key, {"size": 0.0, "wpx": 0.0})
            b["size"] += 1.0
            b["wpx"] += price
        cl: List[Dict[str, float]] = []
        for b in buckets.values():
            cl.append({"price": b["wpx"]/max(1.0,b["size"]), "size": b["size"]})
        cl.sort(key=lambda x: (-x["size"], x["price"]))
        return cl

    r_buckets = cluster_points(highs)[:top]
    s_buckets = cluster_points(lows)[:top]

    resistances = [{"price": round(x["price"], 2), "touches": x["size"]} for x in r_buckets]
    supports    = [{"price": round(x["price"], 2), "touches": x["size"]} for x in s_buckets]

    return {
        "cx": cx, "ticker": ticker, "ts": series[-1][0],
        "mode": "history", "lookback_h": lookback_h,
        "supports": supports, "resistances": resistances
    }
>>>>>>> 635973c (Implement some small fix with database and add order book related tools. #004)

# ====================== SERVER START ======================
if __name__ == "__main__":
    mcp.settings.host = "0.0.0.0"
    mcp.settings.port = 8080  # serves on /mcp

    def _graceful_exit(sig, frame):
        log.info("Stopping (signal: %s)", sig)
        sys.exit(0)

    for _s in ("SIGINT", "SIGTERM"):
        if hasattr(signal, _s):
            signal.signal(getattr(signal, _s), _graceful_exit)

    try:
        mcp.run(transport="streamable-http")
    except KeyboardInterrupt:
        log.info("Stopping (Ctrl-C)")
        sys.exit(0)
