# src/prun-mcp.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PrUn Market MCP Server

A Model Context Protocol (MCP) server that exposes market analysis tools
over HTTP. Provides comprehensive market data analysis, asset management,
and trading tools for Prosperous Universe.

Notes:
- Works with the current public DB schema: prices(cx,ticker,best_bid,best_ask,ts),
  price_history(cx,ticker,bid,ask,ts), materials(ticker,name,category,weight,volume).
- Does not rely on an order-book ("books") table or a "last" column.
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

# Setup logging
log = config.setup_logging("prun-mcp")

mcp = FastMCP(
    "prun-market-mcp",
    instructions="Prosperous Universe market tools for analysis, assets, order planning, and history.",
    stateless_http=True,
)

# -------- DB helpers --------
def db():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con

def pdb():
    con = sqlite3.connect(PRIVATE_DB)
    con.row_factory = sqlite3.Row
    return con

def udb():
    con = sqlite3.connect(USER_DB)
    con.row_factory = sqlite3.Row
    return con

def ts_iso(ts_ms: int) -> str:
    # ts stored as ms in data.py
    return datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc).isoformat()

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def init_local():
    # Public/local helper tables kept minimal. No dependency on 'books'.
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

    # private DB tables mirror prun-private.db from data.py
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

# -------- market utils (no books dependency) --------
def best_row(con, cx: str, ticker: str) -> Tuple[Optional[float], Optional[float]]:
    r = con.execute("SELECT best_bid, best_ask FROM prices WHERE cx=? AND ticker=?",
                    (cx, ticker.upper())).fetchone()
    if not r: return (None, None)
    return (float(r["best_bid"]) if r["best_bid"] is not None else None,
            float(r["best_ask"]) if r["best_ask"] is not None else None)

def mid(bb: Optional[float], ba: Optional[float]) -> Optional[float]:
    if bb is None or ba is None: return None
    return round((bb+ba)/2.0, 1)

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
        return {"ok": True, "db_public": DB_PATH, "db_private": PRIVATE_DB, "db_users": USER_DB,
                "has_prices": has_prices, "has_history": has_hist,
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

# ---- Market: best, spread ----
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
        s = round(ba-bb, 1); m = mid(bb,ba)
        return {"spread": s, "mid": m, "spread_pct": (round(100.0*s/m, 3) if m else None)}
    finally:
        con.close()

# ---- Market: arbitrage ----
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
                "edge_ab": (round(edge_ab,1) if edge_ab is not None else None),
                "buy_at_rev": cx_a, "sell_at_rev": cx_b,
                "edge_ba": (round(edge_ba,1) if edge_ba is not None else None)}
    finally:
        con.close()

# ---- Local Market: arbitrage vs CX ----
@mcp.tool(description="Find LM trades near an exchange: LM sell < CX ask, or LM buy > CX bid.")
def lm_arbitrage_near_cx(
    cx: str,
    planet_ids_csv: Optional[str] = None,   # e.g. "MOR,HRT,ANT"
    min_edge_pct: float = 0.5,              # percent
    limit: int = 50,                        # per side
    tickers_csv: Optional[str] = None,      # optional whitelist
    currency: Optional[str] = None          # PriceCurrency filter (e.g. 'NCC')
) -> Dict[str, Any]:
    con = db()
    con.row_factory = sqlite3.Row
    try:
        # CX bests
        cur = con.execute("SELECT ticker,best_bid,best_ask FROM prices WHERE cx=?", (cx,))
        cxmap = {r["ticker"].upper(): (r["best_bid"], r["best_ask"]) for r in cur.fetchall()}

        # Filters
        params_sell: List[Any] = []
        params_buy:  List[Any] = []

        where_parts = ["Price IS NOT NULL", "MaterialTicker IS NOT NULL"]
        if planet_ids_csv:
            pids = [p.strip() for p in planet_ids_csv.split(",") if p.strip()]
            if pids:
                marks = ",".join("?" for _ in pids)
                where_parts.append(f"PlanetNaturalId IN ({marks})")
                params_sell += pids
                params_buy  += pids
        if tickers_csv:
            tcks = [t.strip().upper() for t in tickers_csv.split(",") if t.strip()]
            if tcks:
                marks = ",".join("?" for _ in tcks)
                where_parts.append(f"UPPER(MaterialTicker) IN ({marks})")
                params_sell += tcks
                params_buy  += tcks
        if currency:
            where_parts.append("PriceCurrency = ?")
            params_sell.append(currency)
            params_buy.append(currency)

        where_sql = " AND ".join(where_parts) if where_parts else "1=1"

        # Pull LM rows
        sell_rows = con.execute(f"""
            SELECT ContractNaturalId,PlanetNaturalId,PlanetName,CreatorCompanyName,CreatorCompanyCode,
                   MaterialName,MaterialTicker,MaterialAmount,Price,PriceCurrency,DeliveryTime,
                   CreationTimeEpochMs,ExpiryTimeEpochMs
            FROM LM_sell
            WHERE {where_sql}
        """, params_sell).fetchall()

        buy_rows = con.execute(f"""
            SELECT ContractNaturalId,PlanetNaturalId,PlanetName,CreatorCompanyName,CreatorCompanyCode,
                   MaterialName,MaterialTicker,MaterialAmount,Price,PriceCurrency,DeliveryTime,
                   CreationTimeEpochMs,ExpiryTimeEpochMs
            FROM LM_buy
            WHERE {where_sql}
        """, params_buy).fetchall()

        def edge_under_ask(row) -> Optional[Dict[str, Any]]:
            t = (row["MaterialTicker"] or "").upper()
            price = row["Price"]
            bb, ba = cxmap.get(t, (None, None))
            if ba is None or ba <= 0 or price is None:
                return None
            if price >= ba:
                return None
            edge_pct = round(100.0 * (ba - price) / ba, 3)
            if edge_pct < min_edge_pct:
                return None
            return {
                "ticker": t,
                "planet_id": row["PlanetNaturalId"],
                "planet": row["PlanetName"],
                "type": "LM_sell_vs_CX_ask",
                "lm_price": price,
                "cx_best_ask": ba,
                "edge_pct": edge_pct,
                "amount": row["MaterialAmount"],
                "currency": row["PriceCurrency"],
                "company": row["CreatorCompanyName"],
                "company_code": row["CreatorCompanyCode"],
                "delivery": row["DeliveryTime"],
                "created": ts_iso(row["CreationTimeEpochMs"]) if row["CreationTimeEpochMs"] else None,
                "expiry": ts_iso(row["ExpiryTimeEpochMs"]) if row["ExpiryTimeEpochMs"] else None,
                "contract_id": row["ContractNaturalId"],
            }

        def edge_over_bid(row) -> Optional[Dict[str, Any]]:
            t = (row["MaterialTicker"] or "").upper()
            price = row["Price"]
            bb, ba = cxmap.get(t, (None, None))
            if bb is None or bb <= 0 or price is None:
                return None
            if price <= bb:
                return None
            edge_pct = round(100.0 * (price - bb) / bb, 3)
            if edge_pct < min_edge_pct:
                return None
            return {
                "ticker": t,
                "planet_id": row["PlanetNaturalId"],
                "planet": row["PlanetName"],
                "type": "LM_buy_vs_CX_bid",
                "lm_price": price,
                "cx_best_bid": bb,
                "edge_pct": edge_pct,
                "amount": row["MaterialAmount"],
                "currency": row["PriceCurrency"],
                "company": row["CreatorCompanyName"],
                "company_code": row["CreatorCompanyCode"],
                "delivery": row["DeliveryTime"],
                "created": ts_iso(row["CreationTimeEpochMs"]) if row["CreationTimeEpochMs"] else None,
                "expiry": ts_iso(row["ExpiryTimeEpochMs"]) if row["ExpiryTimeEpochMs"] else None,
                "contract_id": row["ContractNaturalId"],
            }

        sell_edges = [e for r in sell_rows if (e := edge_under_ask(r))]
        buy_edges  = [e for r in buy_rows  if (e := edge_over_bid(r))]

        sell_edges.sort(key=lambda x: (-x["edge_pct"], x["ticker"], x["planet_id"]))
        buy_edges.sort(key=lambda x: (-x["edge_pct"], x["ticker"], x["planet_id"]))

        return {
            "cx": cx,
            "planet_filter": planet_ids_csv or "ALL",
            "min_edge_pct": min_edge_pct,
            "sell_undercut": sell_edges[:max(1, int(limit))],
            "buy_overbid": buy_edges[:max(1, int(limit))],
            "generated_at": now_iso(),
        }
    finally:
        con.close()

# ---- Materials search ----
@mcp.tool(description="Search for materials on the LM by ticker or name substring.")
def mats_search(q: str, limit: int = 50, category: Optional[str] = None) -> Dict[str, Any]:
    con = db()
    con.row_factory = sqlite3.Row
    try:
        like = f"%{q.strip()}%" if q else "%"
        params: List[Any] = [like.upper(), like.upper()]
        where = "WHERE UPPER(ticker) LIKE ? OR UPPER(name) LIKE ?"
        if category:
            where += " AND UPPER(category)=?"
            params.append(category.strip().upper())
        rows = con.execute(f"""
            SELECT ticker,name,category,weight,volume
            FROM materials
            {where}
            ORDER BY ticker ASC
            LIMIT ?
        """, params + [max(1, int(limit))]).fetchall()
        return {"items": [dict(r) for r in rows], "count": len(rows), "generated_at": now_iso()}
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
                price = {"bid": bb, "ask": ba, "mid": (None if (bb is None or ba is None) else round((bb+ba)/2.0,1))}[method]
            val = (r["qty"]*price) if (price is not None) else None
            if val is not None: total += val
            items.append({"ticker": r["ticker"], "base": r["base"], "qty": r["qty"], "price": price, "value": (round(val,1) if val is not None else None)})
        return {"cx": cx, "method": method, "total_value": round(total,1), "items": items}
    finally:
        con.close()

# ---- History (price_history from data.py, no 'last') ----
def _row_to_hist_dict(r: sqlite3.Row) -> Dict[str, Any]:
    bb = r["bid"]; ba = r["ask"]
    mid_v = (None if (bb is None or ba is None) else round((bb+ba)/2.0, 1))
    spr_v = (None if (bb is None or ba is None) else round(ba-bb, 1))
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
            return {"min": round(min(arr),1), "max": round(max(arr),1), "avg": round(sum(arr)/len(arr),3)}
        return {"cx": cx, "ticker": ticker.upper(), "count": len(rows), "since_ms": cutoff_ms,
                "bid": _agg(bids), "ask": _agg(asks), "spread": _agg(spreads)}
    finally:
        con.close()

# ---- Private user data (username-scoped, aligns with data.py) ----
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
