#!/usr/bin/env python3
"""
PrUn Data Fetcher

A daemon that ingests CSV endpoints from the FNAR API and writes to SQLite databases.

Public Data:
    - /csv/materials → prun.db: materials table
    - /csv/prices → prun.db: prices (latest) and price_history (5-min buckets, 45-day retention)

Private Data (per user from user.db):
    - /csv/inventory → prun-private.db: user_inventory
    - /csv/cxos → prun-private.db: user_cxos  
    - /csv/balances → prun-private.db: user_balances

Features:
    - WAL mode for database durability
    - Automatic retention pruning (45 days for price_history)
    - Comprehensive logging to file.log
    - Header validation and error reporting
    - Per-exchange statistics

Usage:
    python data.py

Configuration:
    Set environment variables in .env file (see .env copy for template)
"""

# Standard library imports
import asyncio
import csv
import io
import logging
import math
import os
import time
from typing import Any, Dict, List, Optional, Tuple

# Third-party imports
import aiohttp
import aiosqlite

# Local imports
from config import config

# -------------------- config --------------------
# Use centralized configuration
PUBLIC_BASE = config.FNAR_API_BASE
PRIVATE_BASE = config.PRIVATE_BASE
POLL_SECS = config.POLL_SECS
RETENTION_SEC = config.RETENTION_SEC

PRUN_DB_PATH = config.DB_PATH
PVT_DB_PATH = config.PRIVATE_DB
USER_DB_PATH = config.USER_DB

EXCHANGES = config.EXCHANGES

# Setup logging using centralized config
log = config.setup_logging("data.py")

# -------------------- utils --------------------
def _now_ms() -> int:
    """Get current timestamp in milliseconds."""
    return int(time.time() * 1000)

def _to_float(x) -> Optional[float]:
    """
    Convert input to float, handling None, empty strings, and 'NA' values.
    
    Args:
        x: Input value to convert
        
    Returns:
        float value or None if conversion fails
    """
    try:
        if x is None: return None
        s = str(x).strip()
        if s == "" or s.upper() == "NA":
            return None
        return float(s)
    except Exception:
        return None

async def fetch_csv(session: aiohttp.ClientSession, url: str) -> List[Dict[str, Any]]:
    """
    Fetch CSV from URL and parse into list[dict].
    Auto-detects comma vs tab delimiter.
    """
    t0 = time.time()
    async with session.get(url, timeout=180) as resp:
        txt = await resp.text()
        dt = time.time() - t0
        if resp.status != 200:
            log.error(f"GET {url} -> {resp.status} len={len(txt)} in {dt:.2f}s")
            return []

        # Delimiter sniffing on first non-empty line
        first_line = next((ln for ln in txt.splitlines() if ln.strip()), "")
        c_commas = first_line.count(",")
        c_tabs = first_line.count("\t")
        if c_commas == 0 and c_tabs == 0:
            # Fallback to csv.Sniffer
            try:
                dialect = csv.Sniffer().sniff(first_line or txt[:1024])
                delim = dialect.delimiter or ","
            except Exception:
                delim = ","
        else:
            delim = "," if c_commas >= c_tabs else "\t"

        f = io.StringIO(txt)
        reader = csv.DictReader(f, delimiter=delim)
        rows = [dict(r) for r in reader]
        headers = list(reader.fieldnames or [])

        if rows:
            log.info(f"GET {url} ok rows={len(rows)} in {dt:.2f}s | headers={headers} | delim='{delim}'")
        else:
            log.info(f"GET {url} ok rows=0 in {dt:.2f}s | headers={headers} | delim='{delim}'")
        return rows

# -------------------- DB init --------------------
PUB_SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS prices(
  cx TEXT NOT NULL,
  ticker TEXT NOT NULL,
  best_bid REAL,
  best_ask REAL,
  ts INTEGER NOT NULL,
  PRIMARY KEY(cx, ticker)
);
CREATE TABLE IF NOT EXISTS price_history(
  cx TEXT NOT NULL,
  ticker TEXT NOT NULL,
  bid REAL,
  ask REAL,
  ts INTEGER NOT NULL,
  PRIMARY KEY(cx, ticker, ts)
);
CREATE TABLE IF NOT EXISTS materials(
  ticker TEXT PRIMARY KEY,
  name TEXT,
  category TEXT,
  weight REAL,
  volume REAL
);
CREATE INDEX IF NOT EXISTS ix_price_history_ts ON price_history(ts);
"""

PVT_SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS user_inventory(
  username TEXT NOT NULL,
  natural_id TEXT,
  name TEXT,
  storage_type TEXT,
  ticker TEXT NOT NULL,
  amount REAL,
  ts INTEGER NOT NULL,
  PRIMARY KEY(username, ticker, natural_id)
);

CREATE TABLE IF NOT EXISTS user_cxos(
  username TEXT NOT NULL,
  order_id TEXT NOT NULL,
  exchange_code TEXT,
  order_type TEXT,
  material_ticker TEXT,
  amount REAL,
  initial_amount REAL,
  price_limit REAL,
  currency TEXT,
  status TEXT,
  created_epoch_ms INTEGER,
  ts INTEGER NOT NULL,
  PRIMARY KEY(username, order_id)
);

CREATE TABLE IF NOT EXISTS user_balances(
  username TEXT NOT NULL,
  currency TEXT NOT NULL,
  amount REAL,
  ts INTEGER NOT NULL,
  PRIMARY KEY(username, currency)
);

-- Ensure ON CONFLICT(...) targets exist on old installs
CREATE UNIQUE INDEX IF NOT EXISTS ux_user_inventory ON user_inventory(username, ticker, natural_id);
CREATE UNIQUE INDEX IF NOT EXISTS ux_user_cxos      ON user_cxos(username, order_id);
CREATE UNIQUE INDEX IF NOT EXISTS ux_user_balances  ON user_balances(username, currency); 
"""

async def init_dbs():
    pub = await aiosqlite.connect(PRUN_DB_PATH)
    await pub.executescript(PUB_SCHEMA); await pub.commit()

    pvt = await aiosqlite.connect(PVT_DB_PATH)
    await pvt.executescript(PVT_SCHEMA); await pvt.commit()

    users = await aiosqlite.connect(USER_DB_PATH)
    # expected schema: users(discord_id TEXT PK, fio_api_key TEXT, username TEXT, created_at INT, updated_at INT)
    return pub, pvt, users

# -------------------- loaders --------------------
async def load_public_materials(session: aiohttp.ClientSession, pub: aiosqlite.Connection):
    url = f"{PUBLIC_BASE}/csv/materials"
    rows = await fetch_csv(session, url)
    if not rows:
        return
    up = 0
    async with pub.execute("BEGIN"):
        for r in rows:
            ticker = (r.get("Ticker") or "").strip().upper()
            if not ticker:
                continue
            name     = (r.get("Name") or "").strip()
            category = (r.get("Category") or "").strip()
            weight   = _to_float(r.get("Weight"))
            volume   = _to_float(r.get("Volume"))
            await pub.execute(
                "INSERT INTO materials(ticker,name,category,weight,volume) VALUES(?,?,?,?,?) "
                "ON CONFLICT(ticker) DO UPDATE SET name=excluded.name, category=excluded.category, weight=excluded.weight, volume=excluded.volume",
                (ticker, name, category, weight, volume)
            )
            up += 1
    await pub.commit()
    log.info(f"materials upserted={up}")

async def load_public_prices(session: aiohttp.ClientSession, pub: aiosqlite.Connection):
    url = f"{PUBLIC_BASE}/csv/prices"
    rows = await fetch_csv(session, url)
    if not rows:
        return

    # header audit
    missing_sets = []
    head = set(rows[0].keys())
    for ex in EXCHANGES:
        need = {f"{ex}-AskPrice", f"{ex}-BidPrice"}
        miss = sorted(list(need - head))
        if miss:
            missing_sets.append((ex, miss))
    if missing_sets:
        for ex, miss in missing_sets:
            log.warning(f"prices header missing for {ex}: {miss}")

    ts = _now_ms()
    up_cnt = 0
    hist_cnt = 0
    # per-exchange stats
    ex_stats: Dict[str, Dict[str,int]] = {ex: {"both":0,"ask_only":0,"bid_only":0,"empty":0} for ex in EXCHANGES}

    async with pub.execute("BEGIN"):
        for r in rows:
            ticker = (r.get("Ticker") or "").strip().upper()
            if not ticker:
                continue
            for ex in EXCHANGES:
                ask = _to_float(r.get(f"{ex}-AskPrice"))
                bid = _to_float(r.get(f"{ex}-BidPrice"))
                if ask is None and bid is None:
                    ex_stats[ex]["empty"] += 1
                    continue
                if ask is not None and bid is not None:
                    ex_stats[ex]["both"] += 1
                elif ask is not None:
                    ex_stats[ex]["ask_only"] += 1
                elif bid is not None:
                    ex_stats[ex]["bid_only"] += 1

                await pub.execute(
                    "INSERT INTO prices(cx,ticker,best_bid,best_ask,ts) VALUES(?,?,?,?,?) "
                    "ON CONFLICT(cx,ticker) DO UPDATE SET best_bid=excluded.best_bid, best_ask=excluded.best_ask, ts=excluded.ts",
                    (ex, ticker, bid, ask, ts)
                )
                await pub.execute(
                    "INSERT OR IGNORE INTO price_history(cx,ticker,bid,ask,ts) VALUES(?,?,?,?,?)",
                    (ex, ticker, bid, ask, ts)
                )
                up_cnt += 1
                hist_cnt += 1
    await pub.commit()

    # retention pruning with count
    cutoff = int((time.time() - RETENTION_SEC) * 1000)
    cur = await pub.execute("SELECT COUNT(*) FROM price_history WHERE ts < ?", (cutoff,))
    old = (await cur.fetchone())[0]
    await pub.execute("DELETE FROM price_history WHERE ts < ?", (cutoff,))
    await pub.commit()

    # log block
    total_tickers = len(rows)
    parts = [f"prices upserted={up_cnt} history_added={hist_cnt} pruned_old_rows={old} cutoff_ts={cutoff} tickers={total_tickers}"]
    for ex in EXCHANGES:
        s = ex_stats[ex]
        parts.append(f"{ex}[both={s['both']},ask_only={s['ask_only']},bid_only={s['bid_only']},empty={s['empty']}]")
    log.info(" | ".join(parts))

async def load_user_inventory(session: aiohttp.ClientSession, pvt: aiosqlite.Connection, username: str, apikey: str) -> int:
    url = f"{PRIVATE_BASE}/csv/inventory?apikey={apikey}&username={username}"
    rows = await fetch_csv(session, url)
    if not rows:
        return 0
    ts = _now_ms()
    up = 0

    # Start a local transaction or a savepoint if one is already open
    sp = None
    try:
        if pvt.in_transaction:
            sp = "inv_sp"
            await pvt.execute(f"SAVEPOINT {sp}")
        else:
            await pvt.execute("BEGIN")

        for r in rows:
            tck = (r.get("Ticker") or "").strip().upper()
            if not tck:
                continue
            await pvt.execute(
                "INSERT INTO user_inventory(username,natural_id,name,storage_type,ticker,amount,ts) VALUES(?,?,?,?,?,?,?) "
                "ON CONFLICT(username,ticker,natural_id) DO UPDATE SET name=excluded.name, storage_type=excluded.storage_type, amount=excluded.amount, ts=excluded.ts",
                (
                    username,
                    (r.get("NaturalId") or "").strip(),
                    (r.get("Name") or "").strip(),
                    (r.get("StorageType") or "").strip(),
                    tck,
                    _to_float(r.get("Amount")),
                    ts,
                ),
            )
            up += 1

        if sp:
            await pvt.execute(f"RELEASE SAVEPOINT {sp}")
        else:
            await pvt.commit()
    except Exception:
        if sp:
            await pvt.execute(f"ROLLBACK TO SAVEPOINT {sp}")
            await pvt.execute(f"RELEASE SAVEPOINT {sp}")
        else:
            await pvt.rollback()
        raise

    log.info(f"inventory[{username}] upserted={up}")
    return up


async def load_user_cxos(session: aiohttp.ClientSession, pvt: aiosqlite.Connection, username: str, apikey: str) -> int:
    url = f"{PRIVATE_BASE}/csv/cxos?apikey={apikey}&username={username}"
    rows = await fetch_csv(session, url)
    if not rows:
        return 0
    ts = _now_ms()
    up = 0

    sp = None
    try:
        if pvt.in_transaction:
            sp = "cx_sp"
            await pvt.execute(f"SAVEPOINT {sp}")
        else:
            await pvt.execute("BEGIN")

        for r in rows:
            oid = (r.get("OrderId") or "").strip()
            if not oid:
                continue
            await pvt.execute(
                "INSERT INTO user_cxos(username,order_id,exchange_code,order_type,material_ticker,amount,initial_amount,price_limit,currency,status,created_epoch_ms,ts) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(username,order_id) DO UPDATE SET exchange_code=excluded.exchange_code, order_type=excluded.order_type, "
                "material_ticker=excluded.material_ticker, amount=excluded.amount, initial_amount=excluded.initial_amount, "
                "price_limit=excluded.price_limit, currency=excluded.currency, status=excluded.status, created_epoch_ms=excluded.created_epoch_ms, ts=excluded.ts",
                (
                    username,
                    oid,
                    (r.get("ExchangeCode") or "").strip().upper(),
                    (r.get("OrderType") or "").strip(),
                    (r.get("MaterialTicker") or "").strip().upper(),
                    _to_float(r.get("Amount")),
                    _to_float(r.get("InitialAmount")),
                    _to_float(r.get("Limit")),
                    (r.get("Currency") or "").strip().upper(),
                    (r.get("Status") or "").strip(),
                    int(_to_float(r.get("CreatedEpochMs")) or 0),
                    ts,
                ),
            )
            up += 1

        if sp:
            await pvt.execute(f"RELEASE SAVEPOINT {sp}")
        else:
            await pvt.commit()
    except Exception:
        if sp:
            await pvt.execute(f"ROLLBACK TO SAVEPOINT {sp}")
            await pvt.execute(f"RELEASE SAVEPOINT {sp}")
        else:
            await pvt.rollback()
        raise

    log.info(f"cxos[{username}] upserted={up}")
    return up


async def load_user_balances(session: aiohttp.ClientSession, pvt: aiosqlite.Connection, username: str, apikey: str) -> int:
    url = f"{PRIVATE_BASE}/csv/balances?apikey={apikey}&username={username}"
    rows = await fetch_csv(session, url)
    if not rows:
        return 0
    ts = _now_ms()
    up = 0

    sp = None
    try:
        if pvt.in_transaction:
            sp = "bal_sp"
            await pvt.execute(f"SAVEPOINT {sp}")
        else:
            await pvt.execute("BEGIN")

        for r in rows:
            cur = (r.get("Currency") or "").strip().upper()
            if not cur:
                continue
            await pvt.execute(
                "INSERT INTO user_balances(username,currency,amount,ts) VALUES(?,?,?,?) "
                "ON CONFLICT(username,currency) DO UPDATE SET amount=excluded.amount, ts=excluded.ts",
                (username, cur, _to_float(r.get("Amount")), ts),
            )
            up += 1

        if sp:
            await pvt.execute(f"RELEASE SAVEPOINT {sp}")
        else:
            await pvt.commit()
    except Exception:
        if sp:
            await pvt.execute(f"ROLLBACK TO SAVEPOINT {sp}")
            await pvt.execute(f"RELEASE SAVEPOINT {sp}")
        else:
            await pvt.rollback()
        raise

    log.info(f"balances[{username}] upserted={up}")
    return up


# -------------------- main loops --------------------
async def load_public(pub):
    t0 = time.time()
    try:
        await load_public_materials(session, pub)
    except Exception as e:
        log.error(f"materials error: {e}")
    try:
        await load_public_prices(session, pub)
    except Exception as e:
        log.error(f"prices error: {e}")
    log.info(f"public cycle took {(time.time()-t0):.2f}s")

async def load_private_all(pvt, usersdb):
    cur = await usersdb.execute("SELECT username, fio_api_key FROM users WHERE fio_api_key IS NOT NULL AND fio_api_key <> ''")
    rows = await cur.fetchall()
    if not rows:
        log.info("private users=0 (no rows in user.db)")
        return
    total_inv = total_cxos = total_bal = 0
    for uname, apikey in rows:
        if not uname or not apikey:
            continue
        try:
            inv = await load_user_inventory(session, pvt, uname, apikey)
            cx  = await load_user_cxos(session, pvt, uname, apikey)
            bal = await load_user_balances(session, pvt, uname, apikey)
            total_inv += inv; total_cxos += cx; total_bal += bal
        except Exception as e:
            log.error(f"private[{uname}] error: {e}")
    log.info(f"private totals inventory={total_inv} cxos={total_cxos} balances={total_bal}")

async def loop_main():
    global session
    log.info(f"BOOT PUBLIC_BASE={PUBLIC_BASE} PRIVATE_BASE={PRIVATE_BASE} POLL_SECS={POLL_SECS} RETENTION_DAYS={RETENTION_SEC/86400:.1f}")
    pub, pvt, users = await init_dbs()
    session = aiohttp.ClientSession()
    try:
        cycle = 0
        while True:
            cycle += 1
            log.info(f"==== cycle {cycle} start ====")
            t0 = time.time()
            await load_public(pub)
            await load_private_all(pvt, users)
            elapsed = time.time() - t0
            sleep_for = max(5, POLL_SECS - int(elapsed))
            log.info(f"==== cycle {cycle} end took {elapsed:.2f}s; sleeping {sleep_for}s ====")
            await asyncio.sleep(sleep_for)
    finally:
        await session.close()
        await pub.close(); await pvt.close(); await users.close()

# re-usable client session
session: aiohttp.ClientSession

if __name__ == "__main__":
    asyncio.run(loop_main())
