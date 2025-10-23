#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PrUn Market MCP Server

A Model Context Protocol (MCP) server that exposes market analysis tools
over HTTP. Provides comprehensive market data analysis, asset management,
and trading tools for Prosperous Universe.

Features:
    - Market analysis: best prices, spreads, depth, arbitrage detection
    - Asset management: holdings, transactions, P&L tracking
    - Order planning: local order management and recommendations
    - Historical data: price history and statistical analysis
    - Private data: user inventory, orders, and balances
    - Materials database: lookup and search functionality

Server:
    - Runs on 0.0.0.0:8080 (/mcp endpoint)
    - Streamable HTTP transport
    - Comprehensive logging to file.log

Usage:
    python prun-mcp.py
    
Dependencies:
    pip install "mcp[cli]" uvicorn

Configuration:
    Set environment variables in .env file (see .env copy for template)
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
# Use centralized configuration
DB_PATH = config.DB_PATH
PRIVATE_DB = config.PRIVATE_DB
USER_DB = config.USER_DB
STATE_DIR = config.STATE_DIR
TICK = config.TICK
HIST_BUCKET_SECS = config.HIST_BUCKET_SECS

# Setup logging using centralized config
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

def ts_iso(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def init_local():
    # public/local helper tables
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

    # private DB tables (mirror of data.py outputs; handle old 'limit' -> 'limit_price')
    pc = pdb(); pcur = pc.cursor()
    pcur.executescript("""
    PRAGMA journal_mode=WAL;

    CREATE TABLE IF NOT EXISTS user_inventory(
      discord_id TEXT NOT NULL,
      username   TEXT NOT NULL,
      natural_id TEXT NOT NULL,
      name       TEXT,
      storage_type TEXT,
      ticker     TEXT NOT NULL,
      amount     REAL NOT NULL,
      ts INTEGER NOT NULL,
      PRIMARY KEY(discord_id, natural_id, ticker)
    );

    CREATE TABLE IF NOT EXISTS user_cxos(
      discord_id TEXT NOT NULL,
      username   TEXT NOT NULL,
      order_id   TEXT NOT NULL,
      exchange_code TEXT NOT NULL,
      order_type TEXT NOT NULL,
      material_ticker TEXT NOT NULL,
      amount     REAL NOT NULL,
      initial_amount REAL,
      limit_price REAL,
      currency   TEXT,
      status     TEXT,
      created_epoch_ms INTEGER,
      ts INTEGER NOT NULL,
      PRIMARY KEY(discord_id, order_id)
    );

    CREATE TABLE IF NOT EXISTS user_balances(
      discord_id TEXT NOT NULL,
      username   TEXT NOT NULL,
      currency   TEXT NOT NULL,
      amount     REAL NOT NULL,
      ts INTEGER NOT NULL,
      PRIMARY KEY(discord_id, currency)
    );
    """)
    # migrate old column name if present
    try:
        cols = [r[1] for r in pcur.execute("PRAGMA table_info(user_cxos)").fetchall()]
        if "limit" in cols and "limit_price" not in cols:
            pcur.execute('ALTER TABLE user_cxos RENAME COLUMN "limit" TO limit_price')
    except Exception as e:
        log.warning(f"user_cxos migration check failed: {e}")
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
def best_row(con, cx: str, ticker: str) -> Tuple[Optional[float], Optional[int], Optional[float], Optional[int]]:
    bb = con.execute("SELECT MAX(price) FROM books WHERE cx=? AND ticker=? AND side='bid'", (cx,ticker)).fetchone()[0]
    ba = con.execute("SELECT MIN(price) FROM books WHERE cx=? AND ticker=? AND side='ask'", (cx,ticker)).fetchone()[0]
    bqty = con.execute("SELECT SUM(qty) FROM books WHERE cx=? AND ticker=? AND side='bid' AND price=?", (cx,ticker,bb)).fetchone()[0] if bb is not None else None
    aqty = con.execute("SELECT SUM(qty) FROM books WHERE cx=? AND ticker=? AND side='ask' AND price=?", (cx,ticker,ba)).fetchone()[0] if ba is not None else None
    return (float(bb) if bb is not None else None,
            int(bqty) if bqty is not None else None,
            float(ba) if ba is not None else None,
            int(aqty) if aqty is not None else None)

def depth_rows(con, cx: str, ticker: str, side: Literal["bid","ask"], n: int) -> List[Dict[str, float]]:
    order = "DESC" if side=="bid" else "ASC"
    rows = con.execute(
        f"SELECT price, SUM(qty) qty FROM books WHERE cx=? AND ticker=? AND side=? "
        f"GROUP BY price ORDER BY price {order} LIMIT ?",
        (cx,ticker,side,n)
    ).fetchall()
    return [{"price": float(r["price"]), "qty": int(r["qty"] or 0)} for r in rows]

def mid(bb: Optional[float], ba: Optional[float]) -> Optional[float]:
    if bb is None or ba is None: return None
    return round((bb+ba)/2.0, 1)

def queue_ahead_calc(con, cx: str, ticker: str, side: Literal["bid","ask"], price: float) -> int:
    if side=="bid":
        better = con.execute("SELECT SUM(qty) FROM books WHERE cx=? AND ticker=? AND side='bid' AND price>?", (cx,ticker,price)).fetchone()[0] or 0
        same   = con.execute("SELECT SUM(qty) FROM books WHERE cx=? AND ticker=? AND side='bid' AND price=?", (cx,ticker,price)).fetchone()[0] or 0
        return int(better + same)
    else:
        better = con.execute("SELECT SUM(qty) FROM books WHERE cx=? AND ticker=? AND side='ask' AND price<?", (cx,ticker,price)).fetchone()[0] or 0
        same   = con.execute("SELECT SUM(qty) FROM books WHERE cx=? AND ticker=? AND side='ask' AND price=?", (cx,ticker,price)).fetchone()[0] or 0
        return int(better + same)

def second_price(con, cx: str, ticker: str, side: Literal["bid","ask"]) -> Optional[float]:
    order = "DESC" if side=="bid" else "ASC"
    rows = con.execute(f"SELECT DISTINCT price FROM books WHERE cx=? AND ticker=? AND side=? ORDER BY price {order} LIMIT 2",
                       (cx,ticker,side)).fetchall()
    return float(rows[1]["price"]) if len(rows)>=2 else None

# ====================== MCP TOOLS ======================

# ---- Health ----
@mcp.tool(description="Quick OK + DB paths + time.")
def health() -> Dict[str, Any]:
    m = db(); p = pdb(); u = udb()
    try:
        has_books  = m.execute("SELECT COUNT(*) FROM sqlite_master WHERE name='books'").fetchone()[0] > 0
        has_prices = m.execute("SELECT COUNT(*) FROM sqlite_master WHERE name='prices'").fetchone()[0] > 0
        has_hist   = m.execute("SELECT COUNT(*) FROM sqlite_master WHERE name='price_history'").fetchone()[0] > 0
        has_priv_i = p.execute("SELECT COUNT(*) FROM sqlite_master WHERE name='user_inventory'").fetchone()[0] > 0
        has_priv_o = p.execute("SELECT COUNT(*) FROM sqlite_master WHERE name='user_cxos'").fetchone()[0] > 0
        has_priv_b = p.execute("SELECT COUNT(*) FROM sqlite_master WHERE name='user_balances'").fetchone()[0] > 0
        has_users  = u.execute("SELECT COUNT(*) FROM sqlite_master WHERE name='users'").fetchone()[0] > 0
        return {"ok": True, "db_public": DB_PATH, "db_private": PRIVATE_DB, "db_users": USER_DB,
                "has_books": has_books, "has_prices": has_prices, "has_history": has_hist,
                "has_private_inventory": has_priv_i, "has_private_cxos": has_priv_o, "has_private_balances": has_priv_b,
                "has_users": has_users, "bucket_secs": HIST_BUCKET_SECS, "time": now_iso()}
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

# ---- Market: best, spread, depth ----
@mcp.tool(description="Best bid/ask + sizes for a ticker on an exchange.")
def market_best(cx: str, ticker: str) -> Dict[str, Any]:
    con = db()
    try:
        bb,bqty,ba,aqty = best_row(con, cx, ticker)
        last = con.execute("SELECT last FROM prices WHERE cx=? AND ticker=?", (cx,ticker)).fetchone()
        return {"best_bid": bb, "best_bid_qty": bqty, "best_ask": ba, "best_ask_qty": aqty,
                "last": (None if not last else last[0])}
    finally:
        con.close()

@mcp.tool(description="Spread and mid for a ticker on an exchange.")
def market_spread(cx: str, ticker: str) -> Dict[str, Any]:
    con = db()
    try:
        bb,_,ba,_ = best_row(con, cx, ticker)
        if bb is None or ba is None:
            return {"spread": None, "mid": None, "spread_pct": None}
        s = round(ba-bb, 1); m = mid(bb,ba)
        return {"spread": s, "mid": m, "spread_pct": (round(100.0*s/m, 3) if m else None)}
    finally:
        con.close()

@mcp.tool(description="Price ladder for one side; n levels.")
def market_depth(cx: str, ticker: str, side: Literal["bid","ask"], n: int = 10) -> Dict[str, Any]:
    con = db()
    try:
        return {"side": side, "levels": depth_rows(con, cx, ticker, side, n)}
    finally:
        con.close()

# Prebuilt depth aliases
for _side in ("bid","ask"):
    for _n in (5,10,25,50,100):
        def _mk(side=_side, n=_n):
            @mcp.tool(name=f"market_depth_{side}_{n}", description=f"Prebuilt depth: side={side}, n={n}.")
            def _alias(cx: str, ticker: str) -> Dict[str, Any]:
                con = db()
                try:
                    return {"side": side, "levels": depth_rows(con, cx, ticker, side, n)}
                finally:
                    con.close()
        _mk()

# ---- Market: imbalance, queue, second, arbitrage, valuation ----
@mcp.tool(description="Top-N depth sums and net imbalance.")
def market_imbalance(cx: str, ticker: str, top: int = 5) -> Dict[str, Any]:
    con = db()
    try:
        b = depth_rows(con, cx, ticker, "bid", top)
        a = depth_rows(con, cx, ticker, "ask", top)
        bqty = sum(x["qty"] for x in b); aqty = sum(x["qty"] for x in a)
        return {"bid_qty": bqty, "ask_qty": aqty, "imbalance": int(bqty - aqty)}
    finally:
        con.close()

@mcp.tool(description="Qty ahead of your order at a given price.")
def market_queue_ahead(cx: str, ticker: str, side: Literal["bid","ask"], price: float, amount: int = 0) -> Dict[str, Any]:
    con = db()
    try:
        ahead = queue_ahead_calc(con, cx, ticker, side, price)
        return {"side": side, "price": price, "ahead_qty": ahead, "amount": amount, "est_position": ahead + (amount if amount>0 else 0)}
    finally:
        con.close()

@mcp.tool(description="Next best level price on the chosen side.")
def market_second_price(cx: str, ticker: str, side: Literal["bid","ask"]) -> Dict[str, Any]:
    con = db()
    try:
        return {"side": side, "second_price": second_price(con, cx, ticker, side)}
    finally:
        con.close()

@mcp.tool(description="Bid-ask edge between two exchanges for one ticker.")
def market_arbitrage_pair(ticker: str, cx_a: str, cx_b: str) -> Dict[str, Any]:
    con = db()
    try:
        a = con.execute("SELECT best_bid,best_ask FROM prices WHERE cx=? AND ticker=?", (cx_a,ticker)).fetchone()
        b = con.execute("SELECT best_bid,best_ask FROM prices WHERE cx=? AND ticker=?", (cx_b,ticker)).fetchone()
        if not a or not b:
            return {"edge_ab": None, "edge_ba": None}
        edge_ab = (a["best_bid"] - b["best_ask"]) if (a["best_bid"] is not None and b["best_ask"] is not None) else None
        edge_ba = (b["best_bid"] - a["best_ask"]) if (b["best_bid"] is not None and a["best_ask"] is not None) else None
        return {"ticker": ticker, "buy_at": cx_b, "sell_at": cx_a,
                "edge_ab": (round(edge_ab,1) if edge_ab is not None else None),
                "buy_at_rev": cx_a, "sell_at_rev": cx_b,
                "edge_ba": (round(edge_ba,1) if edge_ba is not None else None)}
    finally:
        con.close()

@mcp.tool(description="Mark-to-market of local holdings on an exchange.")
def market_value_inventory(cx: str, base: Optional[str] = None, method: Literal["bid","ask","mid"] = "mid") -> Dict[str, Any]:
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

# ---- Market: scoring ----
@mcp.tool(description="Tightness score = 1/spread.")
def market_score_tightness(cx: str, ticker: str) -> Dict[str, Optional[float]]:
    con = db()
    try:
        bb,_,ba,_ = best_row(con, cx, ticker)
        if bb is None or ba is None: return {"score": None}
        spr = ba - bb
        return {"score": None if spr<=0 else round(1.0/spr, 6)}
    finally:
        con.close()

@mcp.tool(description="Spread percentage of mid.")
def market_score_spread_pct(cx: str, ticker: str) -> Dict[str, Optional[float]]:
    con = db()
    try:
        bb,_,ba,_ = best_row(con, cx, ticker); m = mid(bb,ba)
        return {"spread_pct": (round(100.0*(ba-bb)/m, 4) if (bb is not None and ba is not None and m) else None)}
    finally:
        con.close()

@mcp.tool(description="Edge up = bid - ask.")
def market_score_edge_up(cx: str, ticker: str) -> Dict[str, Optional[float]]:
    con = db()
    try:
        bb,_,ba,_ = best_row(con, cx, ticker)
        return {"edge_up": (round((bb - ba),1) if (bb is not None and ba is not None) else None)}
    finally:
        con.close()

@mcp.tool(description="Edge down = ask - bid (spread).")
def market_score_edge_dn(cx: str, ticker: str) -> Dict[str, Optional[float]]:
    con = db()
    try:
        bb,_,ba,_ = best_row(con, cx, ticker)
        return {"edge_dn": (round((ba - bb),1) if (bb is not None and ba is not None) else None)}
    finally:
        con.close()

@mcp.tool(description="Depth hint: total qty across top-N bid/ask.")
def market_score_depth_hint(cx: str, ticker: str, levels: int = 5) -> Dict[str, int]:
    con = db()
    try:
        b = depth_rows(con, cx, ticker, "bid", levels); a = depth_rows(con, cx, ticker, "ask", levels)
        return {"bid_qty": sum(x["qty"] for x in b), "ask_qty": sum(x["qty"] for x in a)}
    finally:
        con.close()

@mcp.tool(description="Widest spreads list on an exchange.")
def market_top_spreads(cx: str, limit: int = 50) -> Dict[str, Any]:
    con = db()
    try:
        rows = con.execute("""SELECT ticker, best_bid, best_ask, (best_ask - best_bid) AS spread
                              FROM prices WHERE cx=? AND best_bid IS NOT NULL AND best_ask IS NOT NULL
                              ORDER BY spread DESC LIMIT ?""", (cx, limit)).fetchall()
        return {"items": [dict(r) for r in rows]}
    finally:
        con.close()

# ---- Materials ----
@mcp.tool(description="Get material name/category/unit by ticker.")
def materials_name(ticker: str) -> Dict[str, Any]:
    con = db()
    try:
        r = con.execute("SELECT ticker,name,category,unit FROM materials WHERE ticker=?", (ticker.upper(),)).fetchone()
        return dict(r) if r else {"error": "ticker not found"}
    finally:
        con.close()

@mcp.tool(description="Search materials by ticker or name prefix.")
def materials_lookup(prefix: str) -> Dict[str, Any]:
    p = f"%{prefix.upper()}%"
    con = db()
    try:
        rows = con.execute(
            "SELECT ticker,name,category,unit FROM materials WHERE UPPER(ticker) LIKE ? OR UPPER(name) LIKE ? ORDER BY ticker LIMIT 50",
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
            val = (r["qty"]*price) if price is not None else None
            if val is not None: total += val
            items.append({"ticker": r["ticker"], "qty": r["qty"], "avg_cost": r["avg_cost"], "price": price, "value": (round(val,1) if val is not None else None)})
        return {"cx": cx, "method": method, "total": round(total,1), "items": items}
    finally:
        con.close()

@mcp.tool(description="PnL of holdings relative to mark.")
def assets_pnl(cx: str, method: Literal["bid","ask","mid"]="mid") -> Dict[str, Any]:
    con = db()
    try:
        rows = con.execute("SELECT ticker, qty, avg_cost FROM assets_holdings").fetchall()
        total=0.0; items=[]
        for r in rows:
            px = con.execute("SELECT best_bid,best_ask FROM prices WHERE cx=? AND ticker=?", (cx, r["ticker"])).fetchone()
            if not px: continue
            bb,ba = px["best_bid"], px["best_ask"]
            price = {"bid": bb, "ask": ba, "mid": (None if (bb is None or ba is None) else round((bb+ba)/2.0,1))}[method]
            if price is None: continue
            pnl = (price - r["avg_cost"]) * r["qty"]
            total += pnl
            items.append({"ticker": r["ticker"], "qty": r["qty"], "avg_cost": r["avg_cost"], "mark": price, "pnl": round(pnl,1)})
        return {"cx": cx, "method": method, "total_pnl": round(total,1), "items": items}
    finally:
        con.close()

@mcp.tool(description="Sell plan to ensure price >= avg_cost*(1+min_profit_pct).")
def assets_sell_plan(ticker: str, qty: float, cx: str, min_profit_pct: float = 2.0) -> Dict[str, Any]:
    con = db()
    try:
        r = con.execute("SELECT qty,avg_cost FROM assets_holdings WHERE ticker=?", (ticker.upper(),)).fetchone()
        if not r or r["qty"] < qty:
            return {"error": "insufficient holding"}
        px = con.execute("SELECT best_bid,best_ask FROM prices WHERE cx=? AND ticker=?", (cx, ticker.upper())).fetchone()
        if not px or px["best_bid"] is None:
            return {"error": "no bid"}
        target_px = max(px["best_bid"], round(r["avg_cost"]*(1.0 + min_profit_pct/100.0)/TICK)*TICK)
        return {"ticker": ticker.upper(), "qty": qty, "target_price": round(target_px,1), "avg_cost": r["avg_cost"], "min_profit_pct": min_profit_pct}
    finally:
        con.close()

# ---- Orders (local planned) ----
@mcp.tool(description="Plan a local order (not sent to FNAR).")
def orders_plan(cx: str, ticker: str, side: Literal["bid","ask"], price: float, qty: float) -> Dict[str, Any]:
    con = db(); cur = con.cursor()
    try:
        cur.execute("""INSERT INTO planned_orders(cx,ticker,side,price,qty,created_at)
                       VALUES(?,?,?,?,?,?)""", (cx, ticker.upper(), side, price, qty, now_iso()))
        con.commit()
        return {"ok": True, "order_id": cur.lastrowid}
    finally:
        con.close()

@mcp.tool(description="List open or partial planned orders.")
def orders_open() -> Dict[str, Any]:
    con = db()
    try:
        rows = con.execute("SELECT * FROM planned_orders WHERE status IN('open','partial') ORDER BY created_at DESC").fetchall()
        return {"items": [dict(r) for r in rows]}
    finally:
        con.close()

@mcp.tool(description="List only bid-side open orders.")
def orders_open_bid() -> Dict[str, Any]:
    con = db()
    try:
        rows = con.execute("SELECT * FROM planned_orders WHERE status IN('open','partial') AND side='bid' ORDER BY created_at DESC").fetchall()
        return {"items": [dict(r) for r in rows]}
    finally:
        con.close()

@mcp.tool(description="Large ask orders by CX with min qty=100 by default.")
def orders_open_ask_min(cx: str, min_qty: float = 100.0) -> Dict[str, Any]:
    con = db()
    try:
        rows = con.execute("""SELECT * FROM planned_orders
                              WHERE status IN('open','partial') AND side='ask' AND cx=? AND qty>=?
                              ORDER BY created_at DESC""", (cx, min_qty)).fetchall()
        return {"items": [dict(r) for r in rows]}
    finally:
        con.close()

@mcp.tool(description="Get one planned order.")
def orders_detail(order_id: int) -> Dict[str, Any]:
    con = db()
    try:
        r = con.execute("SELECT * FROM planned_orders WHERE id=?", (order_id,)).fetchone()
        return dict(r) if r else {"error": "order not found"}
    finally:
        con.close()

@mcp.tool(description="Open-order notional by side, optional CX filter.")
def orders_value(cx: Optional[str] = None) -> Dict[str, Any]:
    con = db()
    try:
        where = "WHERE status IN('open','partial')"; params=[]
        if cx: where += " AND cx=?"; params.append(cx)
        rows = con.execute(f"SELECT side, SUM((qty - filled_qty)*price) notional FROM planned_orders {where} GROUP BY side", params).fetchall()
        out = {r["side"]: round(r["notional"] or 0.0,1) for r in rows}
        return {"notional": out}
    finally:
        con.close()

@mcp.tool(description="Suggest price move using best, second price, and queue-ahead.")
def orders_recommend_move(order_id: int) -> Dict[str, Any]:
    con = db()
    try:
        o = con.execute("SELECT * FROM planned_orders WHERE id=?", (order_id,)).fetchone()
        if not o: return {"error": "order not found"}
        bb,bqty,ba,aqty = best_row(con, o["cx"], o["ticker"])
        sec = second_price(con, o["cx"], o["ticker"], o["side"])
        ahead = queue_ahead_calc(con, o["cx"], o["ticker"], o["side"], o["price"])
        if o["side"]=="bid":
            suggestion = min((bb or o["price"]), (sec or o["price"]))
            new_px = round(max(o["price"], suggestion)/TICK)*TICK
        else:
            suggestion = max((ba or o["price"]), (sec or o["price"]))
            new_px = round(min(o["price"], suggestion)/TICK)*TICK
        return {
            "order": dict(o),
            "best": {"bid": bb, "bid_qty": bqty, "ask": ba, "ask_qty": aqty},
            "second_price": sec,
            "queue_ahead_qty": ahead,
            "suggest_price": round(new_px,1)
        }
    finally:
        con.close()

# ---- History (price_history from data.py) ----
def _row_to_hist_dict(r: sqlite3.Row) -> Dict[str, Any]:
    bb = r["best_bid"]; ba = r["best_ask"]; last = r["last"]
    mid_v = (None if (bb is None or ba is None) else round((bb+ba)/2.0, 1))
    spr_v = (None if (bb is None or ba is None) else round(ba-bb, 1))
    return {
        "ts": int(r["ts"]),
        "time": ts_iso(r["ts"]),
        "best_bid": bb,
        "best_ask": ba,
        "last": last,
        "mid": mid_v,
        "spread": spr_v,
    }

@mcp.tool(description="Latest N history rows for cx,ticker. Returns ascending by ts.")
def history_latest(cx: str, ticker: str, n: int = 288) -> Dict[str, Any]:
    con = db()
    try:
        rows = con.execute(
            "SELECT ts,best_bid,best_ask,last FROM price_history WHERE cx=? AND ticker=? ORDER BY ts DESC LIMIT ?",
            (cx, ticker.upper(), int(n))
        ).fetchall()
        items = [_row_to_hist_dict(r) for r in reversed(rows)]
        return {"cx": cx, "ticker": ticker.upper(), "bucket_secs": HIST_BUCKET_SECS, "rows": items}
    finally:
        con.close()

@mcp.tool(description="History since N minutes ago for cx,ticker. Returns ascending by ts.")
def history_since(cx: str, ticker: str, minutes: int = 60) -> Dict[str, Any]:
    con = db()
    try:
        cutoff = int(datetime.now(timezone.utc).timestamp() - minutes*60)
        rows = con.execute(
            "SELECT ts,best_bid,best_ask,last FROM price_history WHERE cx=? AND ticker=? AND ts>=? ORDER BY ts ASC",
            (cx, ticker.upper(), cutoff)
        ).fetchall()
        items = [_row_to_hist_dict(r) for r in rows]
        return {"cx": cx, "ticker": ticker.upper(), "since": cutoff, "bucket_secs": HIST_BUCKET_SECS, "rows": items}
    finally:
        con.close()

@mcp.tool(description="History between [start_ts,end_ts] for cx,ticker. Returns ascending by ts.")
def history_between(cx: str, ticker: str, start_ts: int, end_ts: int, limit: int = 10000) -> Dict[str, Any]:
    if end_ts < start_ts:
        return {"error": "end_ts < start_ts"}
    con = db()
    try:
        rows = con.execute(
            "SELECT ts,best_bid,best_ask,last FROM price_history WHERE cx=? AND ticker=? AND ts BETWEEN ? AND ? ORDER BY ts ASC LIMIT ?",
            (cx, ticker.upper(), int(start_ts), int(end_ts), int(limit))
        ).fetchall()
        items = [_row_to_hist_dict(r) for r in rows]
        return {"cx": cx, "ticker": ticker.upper(), "start_ts": int(start_ts), "end_ts": int(end_ts),
                "bucket_secs": HIST_BUCKET_SECS, "rows": items}
    finally:
        con.close()

@mcp.tool(description="Basic stats over history since N minutes: min/max/avg for bid, ask, last, spread.")
def history_stats(cx: str, ticker: str, minutes: int = 1440) -> Dict[str, Any]:
    con = db()
    try:
        cutoff = int(datetime.now(timezone.utc).timestamp() - minutes*60)
        rows = con.execute(
            "SELECT best_bid,best_ask,last FROM price_history WHERE cx=? AND ticker=? AND ts>=? ORDER BY ts ASC",
            (cx, ticker.upper(), cutoff)
        ).fetchall()
        if not rows:
            return {"cx": cx, "ticker": ticker.upper(), "count": 0}
        bids = [r["best_bid"] for r in rows if r["best_bid"] is not None]
        asks = [r["best_ask"] for r in rows if r["best_ask"] is not None]
        lasts = [r["last"] for r in rows if r["last"] is not None]
        spreads = [(r["best_ask"] - r["best_bid"]) for r in rows if (r["best_bid"] is not None and r["best_ask"] is not None)]
        def _agg(arr):
            if not arr: return {"min": None, "max": None, "avg": None}
            return {"min": round(min(arr),1), "max": round(max(arr),1), "avg": round(sum(arr)/len(arr),3)}
        return {
            "cx": cx, "ticker": ticker.upper(), "count": len(rows), "since": cutoff,
            "bid": _agg(bids), "ask": _agg(asks), "last": _agg(lasts), "spread": _agg(spreads)
        }
    finally:
        con.close()

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

@mcp.tool(description="User inventory. Optional filters: ticker, storage_type, name_like, min_amount.")
def user_inventory(discord_id: str, ticker: Optional[str]=None, storage_type: Optional[str]=None,
                   name_like: Optional[str]=None, min_amount: float = 0.0, limit: int = 500) -> Dict[str, Any]:
    con = pdb()
    try:
        where = ["discord_id=?"]; args=[discord_id]
        if ticker: where.append("ticker=?"); args.append(ticker.upper())
        if storage_type: where.append("storage_type=?"); args.append(storage_type)
        if name_like: where.append("UPPER(name) LIKE ?"); args.append(f"%{name_like.upper()}%")
        if min_amount > 0: where.append("amount>=?"); args.append(min_amount)
        sql = "SELECT username,natural_id,name,storage_type,ticker,amount,ts FROM user_inventory WHERE " + " AND ".join(where) + " ORDER BY ticker,name LIMIT ?"
        args.append(int(limit))
        rows = con.execute(sql, args).fetchall()
        return {"discord_id": discord_id, "rows": [dict(r) for r in rows]}
    finally:
        con.close()

@mcp.tool(description="User CXOS orders. Optional filters: status, exchange_code, ticker.")
def user_cxos(discord_id: str, status: Optional[str]=None, exchange_code: Optional[str]=None,
              ticker: Optional[str]=None, limit: int = 500) -> Dict[str, Any]:
    con = pdb()
    try:
        where = ["discord_id=?"]; args=[discord_id]
        if status: where.append("status=?"); args.append(status.upper())
        if exchange_code: where.append("exchange_code=?"); args.append(exchange_code.upper())
        if ticker: where.append("material_ticker=?"); args.append(ticker.upper())
        sql = ("SELECT username,order_id,exchange_code,order_type,material_ticker,amount,initial_amount,limit_price AS limit,"
               "currency,status,created_epoch_ms,ts FROM user_cxos WHERE " + " AND ".join(where) +
               " ORDER BY created_epoch_ms DESC LIMIT ?")
        args.append(int(limit))
        rows = con.execute(sql, args).fetchall()
        return {"discord_id": discord_id, "rows": [dict(r) for r in rows]}
    finally:
        con.close()

@mcp.tool(description="User balances by currency.")
def user_balances(discord_id: str) -> Dict[str, Any]:
    con = pdb()
    try:
        rows = con.execute("SELECT username,currency,amount,ts FROM user_balances WHERE discord_id=? ORDER BY currency", (discord_id,)).fetchall()
        return {"discord_id": discord_id, "rows": [dict(r) for r in rows]}
    finally:
        con.close()

@mcp.tool(description="List open-ish orders for a user. Status in (OPEN, ACTIVE, PARTIAL, PLACED).")
def list_open_orders(discord_id: str, limit: int = 500) -> Dict[str, Any]:
    con = pdb()
    try:
        statuses = ("OPEN","ACTIVE","PARTIAL","PLACED")
        q = f"""SELECT username,order_id,exchange_code,order_type,material_ticker,amount,initial_amount,
                limit_price AS limit,currency,status,created_epoch_ms,ts
                FROM user_cxos
                WHERE discord_id=? AND status IN ({','.join('?'*len(statuses))})
                ORDER BY created_epoch_ms DESC LIMIT ?"""
        rows = con.execute(q, (discord_id, *statuses, int(limit))).fetchall()
        return {"discord_id": discord_id, "rows": [dict(r) for r in rows]}
    finally:
        con.close()

@mcp.tool(description="Aggregate user positions by ticker from inventory table.")
def user_positions_summary(discord_id: str) -> Dict[str, Any]:
    con = pdb()
    try:
        rows = con.execute("""SELECT ticker, SUM(amount) AS qty
                              FROM user_inventory WHERE discord_id=?
                              GROUP BY ticker ORDER BY ticker""", (discord_id,)).fetchall()
        return {"discord_id": discord_id, "positions": [{"ticker": r["ticker"], "qty": float(r["qty"] or 0)} for r in rows]}
    finally:
        con.close()

# ====================== SERVER START ======================
if __name__ == "__main__":
    import signal

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
