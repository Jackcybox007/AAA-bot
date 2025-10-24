#!/usr/bin/env python3
"""
PrUn Data Fetcher

Public:
  - /csv/materials → prun.db: materials
  - /csv/prices    → prun.db: prices, price_history
  - /csv/planets   → first column as PlanetNaturalId
      /csv/localmarket/buy/{planetID}  → prun.db: LM_buy
      /csv/localmarket/sell/{planetID} → prun.db: LM_sell
      /csv/localmarket/ship/{planetID} → prun.db: LM_ship
        Header:
          ContractNaturalId,PlanetNaturalId,PlanetName,
          OriginPlanetNaturalId,OriginPlanetName,
          DestinationPlanetNaturalId,DestinationPlanetName,
          CargoWeight,CargoVolume,
          CreatorCompanyName,CreatorCompanyCode,
          PayoutPrice,PayoutCurrency,DeliveryTime,
          CreationTimeEpochMs,ExpiryTimeEpochMs,MinimumRating

Private (per user from user.db):
  - /csv/inventory → prun-private.db: user_inventory
  - /csv/cxos      → prun-private.db: user_cxos
  - /csv/balances  → prun-private.db: user_balances

Notes:
  - Only /buy, /sell, /ship are used. No /bu fallback.
  - Extra planet IDs appended: MOR, HRT, ANT, BEN, HUB, ARC.
"""

import asyncio
import csv
import io
import logging
import time
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import aiosqlite

from config import config

# ---------- config ----------
PUBLIC_BASE = config.FNAR_API_BASE
PRIVATE_BASE = config.PRIVATE_BASE
POLL_SECS = config.POLL_SECS
RETENTION_SEC = config.RETENTION_SEC

PRUN_DB_PATH = config.DB_PATH
PVT_DB_PATH = config.PRIVATE_DB
USER_DB_PATH = config.USER_DB

EXCHANGES = config.EXCHANGES
LM_EXTRA_PLANET_IDS = ["MOR", "HRT", "ANT", "BEN", "HUB", "ARC"]

log = config.setup_logging("data.py")

# ---------- utils ----------
def _now_ms() -> int:
    return int(time.time() * 1000)

def _to_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        s = str(x).strip()
        if s == "" or s.upper() == "NA":
            return None
        return float(s)
    except Exception:
        return None

def _to_int(x) -> Optional[int]:
    try:
        f = _to_float(x)
        return int(f) if f is not None else None
    except Exception:
        return None

def _to_str(x) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s if s != "" else None

async def fetch_csv(session: aiohttp.ClientSession, url: str) -> List[Dict[str, Any]]:
    t0 = time.time()
    async with session.get(url, timeout=180) as resp:
        txt = await resp.text()
        dt = time.time() - t0
        if resp.status != 200:
            log.error(f"GET {url} -> {resp.status} len={len(txt)} in {dt:.2f}s")
            return []

        # Delimiter sniff
        first_line = next((ln for ln in txt.splitlines() if ln.strip()), "")
        c_commas = first_line.count(",")
        c_tabs = first_line.count("\t")
        if c_commas == 0 and c_tabs == 0:
            try:
                dialect = csv.Sniffer().sniff(first_line or txt[:1024])
                delim = dialect.delimiter or ","
            except Exception:
                delim = ","
        else:
            delim = "," if c_commas >= c_tabs else "\t"

        reader = csv.DictReader(io.StringIO(txt), delimiter=delim)
        rows = [dict(r) for r in reader]
        headers = list(reader.fieldnames or [])
        log.info(f"GET {url} ok rows={len(rows)} in {dt:.2f}s | headers={headers} | delim='{delim}'")
        return rows

# ---------- DB init (public) ----------
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

CREATE TABLE IF NOT EXISTS LM_buy(
  ContractNaturalId     TEXT PRIMARY KEY,
  PlanetNaturalId       TEXT,
  PlanetName            TEXT,
  CreatorCompanyName    TEXT,
  CreatorCompanyCode    TEXT,
  MaterialName          TEXT,
  MaterialTicker        TEXT,
  MaterialCategory      TEXT,
  MaterialWeight        REAL,
  MaterialVolume        REAL,
  MaterialAmount        REAL,
  Price                 REAL,
  PriceCurrency         TEXT,
  DeliveryTime          TEXT,
  CreationTimeEpochMs   INTEGER,
  ExpiryTimeEpochMs     INTEGER,
  MinimumRating         REAL
);
CREATE TABLE IF NOT EXISTS LM_sell(
  ContractNaturalId     TEXT PRIMARY KEY,
  PlanetNaturalId       TEXT,
  PlanetName            TEXT,
  CreatorCompanyName    TEXT,
  CreatorCompanyCode    TEXT,
  MaterialName          TEXT,
  MaterialTicker        TEXT,
  MaterialCategory      TEXT,
  MaterialWeight        REAL,
  MaterialVolume        REAL,
  MaterialAmount        REAL,
  Price                 REAL,
  PriceCurrency         TEXT,
  DeliveryTime          TEXT,
  CreationTimeEpochMs   INTEGER,
  ExpiryTimeEpochMs     INTEGER,
  MinimumRating         REAL
);
CREATE INDEX IF NOT EXISTS ix_lm_buy_planet   ON LM_buy(PlanetNaturalId);
CREATE INDEX IF NOT EXISTS ix_lm_buy_ticker   ON LM_buy(MaterialTicker);
CREATE INDEX IF NOT EXISTS ix_lm_sell_planet  ON LM_sell(PlanetNaturalId);
CREATE INDEX IF NOT EXISTS ix_lm_sell_ticker  ON LM_sell(MaterialTicker);
"""

# LM_ship expected and DDL
LM_SHIP_EXPECTED_COLS = [
  "ContractNaturalId","PlanetNaturalId","PlanetName",
  "OriginPlanetNaturalId","OriginPlanetName",
  "DestinationPlanetNaturalId","DestinationPlanetName",
  "CargoWeight","CargoVolume",
  "CreatorCompanyName","CreatorCompanyCode",
  "PayoutPrice","PayoutCurrency","DeliveryTime",
  "CreationTimeEpochMs","ExpiryTimeEpochMs","MinimumRating",
]

LM_SHIP_SCHEMA = """
CREATE TABLE IF NOT EXISTS LM_ship(
  ContractNaturalId           TEXT PRIMARY KEY,
  PlanetNaturalId             TEXT,
  PlanetName                  TEXT,
  OriginPlanetNaturalId       TEXT,
  OriginPlanetName            TEXT,
  DestinationPlanetNaturalId  TEXT,
  DestinationPlanetName       TEXT,
  CargoWeight                 REAL,
  CargoVolume                 REAL,
  CreatorCompanyName          TEXT,
  CreatorCompanyCode          TEXT,
  PayoutPrice                 REAL,
  PayoutCurrency              TEXT,
  DeliveryTime                TEXT,
  CreationTimeEpochMs         INTEGER,
  ExpiryTimeEpochMs           INTEGER,
  MinimumRating               REAL
);
CREATE INDEX IF NOT EXISTS ix_lm_ship_planet       ON LM_ship(PlanetNaturalId);
CREATE INDEX IF NOT EXISTS ix_lm_ship_origin       ON LM_ship(OriginPlanetNaturalId);
CREATE INDEX IF NOT EXISTS ix_lm_ship_destination  ON LM_ship(DestinationPlanetNaturalId);
"""

async def _ensure_table(conn: aiosqlite.Connection, name: str, expected_cols: List[str], create_sql: str):
    cols: List[str] = []
    async with conn.execute(f"PRAGMA table_info({name})") as cur:
        async for row in cur:
            cols.append(row[1])  # name
    if cols and cols != expected_cols:
        log.warning(f"{name} schema mismatch. Recreating. old_cols={cols} new_cols={expected_cols}")
        await conn.execute(f"DROP TABLE IF EXISTS {name}")
        await conn.commit()
    await conn.executescript(create_sql)
    await conn.commit()

async def ensure_lm_ship_schema(pub: aiosqlite.Connection):
    await _ensure_table(pub, "LM_ship", LM_SHIP_EXPECTED_COLS, LM_SHIP_SCHEMA)

# ---------- DB init (private) ----------
PVT_EXPECTED = {
  "user_inventory": [
    "discord_id","username","natural_id","name","storage_type",
    "ticker","amount","ts"
  ],
  "user_cxos": [
    "discord_id","username","order_id","exchange_code","order_type",
    "material_ticker","amount","initial_amount","price_limit","currency",
    "status","created_epoch_ms","ts"
  ],
  "user_balances": [
    "discord_id","username","currency","amount","ts"
  ],
}

PVT_SCHEMA_INV = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS user_inventory(
  discord_id   TEXT NOT NULL,
  username     TEXT NOT NULL,
  natural_id   TEXT,
  name         TEXT,
  storage_type TEXT,
  ticker       TEXT NOT NULL,
  amount       REAL,
  ts           INTEGER NOT NULL,
  PRIMARY KEY(discord_id, ticker, natural_id)
);
"""

PVT_SCHEMA_CXOS = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS user_cxos(
  discord_id       TEXT NOT NULL,
  username         TEXT NOT NULL,
  order_id         TEXT NOT NULL,
  exchange_code    TEXT,
  order_type       TEXT,
  material_ticker  TEXT,
  amount           REAL,
  initial_amount   REAL,
  price_limit      REAL,
  currency         TEXT,
  status           TEXT,
  created_epoch_ms INTEGER,
  ts               INTEGER NOT NULL,
  PRIMARY KEY(discord_id, order_id)
);
"""

PVT_SCHEMA_BAL = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS user_balances(
  discord_id TEXT NOT NULL,
  username   TEXT NOT NULL,
  currency   TEXT NOT NULL,
  amount     REAL,
  ts         INTEGER NOT NULL,
  PRIMARY KEY(discord_id, currency)
);
"""

async def ensure_private_schema(pvt: aiosqlite.Connection):
    await _ensure_table(pvt, "user_inventory", PVT_EXPECTED["user_inventory"], PVT_SCHEMA_INV)
    await _ensure_table(pvt, "user_cxos",      PVT_EXPECTED["user_cxos"],      PVT_SCHEMA_CXOS)
    await _ensure_table(pvt, "user_balances",  PVT_EXPECTED["user_balances"],  PVT_SCHEMA_BAL)

async def init_dbs():
    pub = await aiosqlite.connect(PRUN_DB_PATH)
    await pub.executescript(PUB_SCHEMA); await pub.commit()
    await ensure_lm_ship_schema(pub)

    pvt = await aiosqlite.connect(PVT_DB_PATH)
    await ensure_private_schema(pvt)

    users = await aiosqlite.connect(USER_DB_PATH)
    return pub, pvt, users

# ---------- loaders: PUBLIC ----------
async def load_public_materials(session: aiohttp.ClientSession, pub: aiosqlite.Connection):
    rows = await fetch_csv(session, f"{PUBLIC_BASE}/csv/materials")
    if not rows:
        return
    await pub.execute("BEGIN")
    for r in rows:
        ticker = (r.get("Ticker") or "").strip().upper()
        if not ticker:
            continue
        await pub.execute(
            "INSERT INTO materials(ticker,name,category,weight,volume) VALUES(?,?,?,?,?) "
            "ON CONFLICT(ticker) DO UPDATE SET name=excluded.name, category=excluded.category, weight=excluded.weight, volume=excluded.volume",
            (
                ticker,
                (r.get("Name") or "").strip(),
                (r.get("Category") or "").strip(),
                _to_float(r.get("Weight")),
                _to_float(r.get("Volume")),
            ),
        )
    await pub.commit()
    log.info("materials upserted")

async def load_public_prices(session: aiohttp.ClientSession, pub: aiosqlite.Connection):
    rows = await fetch_csv(session, f"{PUBLIC_BASE}/csv/prices")
    if not rows:
        return
    head = set(rows[0].keys())
    for ex in EXCHANGES:
        for k in (f"{ex}-AskPrice", f"{ex}-BidPrice"):
            if k not in head:
                log.warning(f"prices header missing: {k}")
    ts = _now_ms()
    await pub.execute("BEGIN")
    for r in rows:
        ticker = (r.get("Ticker") or "").strip().upper()
        if not ticker:
            continue
        for ex in EXCHANGES:
            ask = _to_float(r.get(f"{ex}-AskPrice"))
            bid = _to_float(r.get(f"{ex}-BidPrice"))
            if ask is None and bid is None:
                continue
            await pub.execute(
                "INSERT INTO prices(cx,ticker,best_bid,best_ask,ts) VALUES(?,?,?,?,?) "
                "ON CONFLICT(cx,ticker) DO UPDATE SET best_bid=excluded.best_bid, best_ask=excluded.best_ask, ts=excluded.ts",
                (ex, ticker, bid, ask, ts)
            )
            await pub.execute(
                "INSERT OR IGNORE INTO price_history(cx,ticker,bid,ask,ts) VALUES(?,?,?,?,?)",
                (ex, ticker, bid, ask, ts)
            )
    await pub.commit()
    cutoff = int((time.time() - RETENTION_SEC) * 1000)
    await pub.execute("DELETE FROM price_history WHERE ts < ?", (cutoff,))
    await pub.commit()

# ---- Local Market (planets → buy/sell/ship) ----
async def _planet_ids(session: aiohttp.ClientSession) -> List[str]:
    rows = await fetch_csv(session, f"{PUBLIC_BASE}/csv/planets")
    ids: List[str] = []
    if rows:
        first_key = list(rows[0].keys())[0]
        for r in rows:
            v = _to_str(r.get(first_key))
            if v:
                ids.append(v)
    ids.extend(LM_EXTRA_PLANET_IDS)
    uniq = sorted(set(ids))
    log.info(f"planets ids={len(uniq)} + extras={LM_EXTRA_PLANET_IDS}")
    return uniq

async def _fetch_lm_kind(session: aiohttp.ClientSession, pid: str, kind: str) -> List[Dict[str, Any]]:
    url = f"{PUBLIC_BASE}/csv/localmarket/{kind}/{pid}"
    return await fetch_csv(session, url) or []

def _lm_tuple(r: Dict[str, Any]) -> Tuple:
    return (
        _to_str(r.get("ContractNaturalId")),
        _to_str(r.get("PlanetNaturalId")),
        _to_str(r.get("PlanetName")),
        _to_str(r.get("CreatorCompanyName")),
        _to_str(r.get("CreatorCompanyCode")),
        _to_str(r.get("MaterialName")),
        _to_str(r.get("MaterialTicker")),
        _to_str(r.get("MaterialCategory")),
        _to_float(r.get("MaterialWeight")),
        _to_float(r.get("MaterialVolume")),
        _to_float(r.get("MaterialAmount")),
        _to_float(r.get("Price")),
        _to_str(r.get("PriceCurrency")),
        _to_str(r.get("DeliveryTime")),
        _to_int(r.get("CreationTimeEpochMs")),
        _to_int(r.get("ExpiryTimeEpochMs")),
        _to_float(r.get("MinimumRating")),
    )

LM_COLS = (
    "ContractNaturalId,PlanetNaturalId,PlanetName,CreatorCompanyName,CreatorCompanyCode,"
    "MaterialName,MaterialTicker,MaterialCategory,MaterialWeight,MaterialVolume,MaterialAmount,"
    "Price,PriceCurrency,DeliveryTime,CreationTimeEpochMs,ExpiryTimeEpochMs,MinimumRating"
)
LM_PLACEH = ",".join(["?"] * 17)

LM_UPSERT_BUY = f"""
INSERT INTO LM_buy({LM_COLS}) VALUES({LM_PLACEH})
ON CONFLICT(ContractNaturalId) DO UPDATE SET
  PlanetNaturalId=excluded.PlanetNaturalId,
  PlanetName=excluded.PlanetName,
  CreatorCompanyName=excluded.CreatorCompanyName,
  CreatorCompanyCode=excluded.CreatorCompanyCode,
  MaterialName=excluded.MaterialName,
  MaterialTicker=excluded.MaterialTicker,
  MaterialCategory=excluded.MaterialCategory,
  MaterialWeight=excluded.MaterialWeight,
  MaterialVolume=excluded.MaterialVolume,
  MaterialAmount=excluded.MaterialAmount,
  Price=excluded.Price,
  PriceCurrency=excluded.PriceCurrency,
  DeliveryTime=excluded.DeliveryTime,
  CreationTimeEpochMs=excluded.CreationTimeEpochMs,
  ExpiryTimeEpochMs=excluded.ExpiryTimeEpochMs,
  MinimumRating=excluded.MinimumRating
"""
LM_UPSERT_SELL = LM_UPSERT_BUY.replace("LM_buy", "LM_sell")

def _lm_ship_tuple(r: Dict[str, Any]) -> Tuple:
    return (
        _to_str(r.get("ContractNaturalId")),
        _to_str(r.get("PlanetNaturalId")),
        _to_str(r.get("PlanetName")),
        _to_str(r.get("OriginPlanetNaturalId")),
        _to_str(r.get("OriginPlanetName")),
        _to_str(r.get("DestinationPlanetNaturalId")),
        _to_str(r.get("DestinationPlanetName")),
        _to_float(r.get("CargoWeight")),
        _to_float(r.get("CargoVolume")),
        _to_str(r.get("CreatorCompanyName")),
        _to_str(r.get("CreatorCompanyCode")),
        _to_float(r.get("PayoutPrice")),
        _to_str(r.get("PayoutCurrency")),
        _to_str(r.get("DeliveryTime")),
        _to_int(r.get("CreationTimeEpochMs")),
        _to_int(r.get("ExpiryTimeEpochMs")),
        _to_float(r.get("MinimumRating")),
    )

LM_SHIP_COLS = (
  "ContractNaturalId,PlanetNaturalId,PlanetName,"
  "OriginPlanetNaturalId,OriginPlanetName,"
  "DestinationPlanetNaturalId,DestinationPlanetName,"
  "CargoWeight,CargoVolume,"
  "CreatorCompanyName,CreatorCompanyCode,"
  "PayoutPrice,PayoutCurrency,DeliveryTime,"
  "CreationTimeEpochMs,ExpiryTimeEpochMs,MinimumRating"
)
LM_SHIP_PLACEH = ",".join(["?"] * 17)

LM_UPSERT_SHIP = f"""
INSERT INTO LM_ship({LM_SHIP_COLS}) VALUES({LM_SHIP_PLACEH})
ON CONFLICT(ContractNaturalId) DO UPDATE SET
  PlanetNaturalId=excluded.PlanetNaturalId,
  PlanetName=excluded.PlanetName,
  OriginPlanetNaturalId=excluded.OriginPlanetNaturalId,
  OriginPlanetName=excluded.OriginPlanetName,
  DestinationPlanetNaturalId=excluded.DestinationPlanetNaturalId,
  DestinationPlanetName=excluded.DestinationPlanetName,
  CargoWeight=excluded.CargoWeight,
  CargoVolume=excluded.CargoVolume,
  CreatorCompanyName=excluded.CreatorCompanyName,
  CreatorCompanyCode=excluded.CreatorCompanyCode,
  PayoutPrice=excluded.PayoutPrice,
  PayoutCurrency=excluded.PayoutCurrency,
  DeliveryTime=excluded.DeliveryTime,
  CreationTimeEpochMs=excluded.CreationTimeEpochMs,
  ExpiryTimeEpochMs=excluded.ExpiryTimeEpochMs,
  MinimumRating=excluded.MinimumRating
"""

async def load_public_localmarket(session: aiohttp.ClientSession, pub: aiosqlite.Connection):
    pid_list = await _planet_ids(session)
    if not pid_list:
        log.info("localmarket: planets=0")
        return

    sem = asyncio.Semaphore(8)

    async def _fetch(pid: str):
        async with sem:
            br = await _fetch_lm_kind(session, pid, "buy")
            sr = await _fetch_lm_kind(session, pid, "sell")
            hr = await _fetch_lm_kind(session, pid, "ship")
            return pid, br, sr, hr

    tasks = [asyncio.create_task(_fetch(pid)) for pid in pid_list]
    results: List[Tuple[str, List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]] = []

    for t in asyncio.as_completed(tasks):
        try:
            results.append(await t)
        except Exception as e:
            log.error(f"lm fetch task error: {e}")

    buy_rows = sum((len(b) for _, b, _, _ in results), 0)
    sell_rows = sum((len(s) for _, _, s, _ in results), 0)
    ship_rows = sum((len(h) for _, _, _, h in results), 0)
    log.info(f"localmarket fetched: planets={len(results)} buy={buy_rows} sell={sell_rows} ship={ship_rows}")

    if not results:
        return

    await pub.execute("BEGIN")
    b_up = s_up = h_up = 0
    for _, b_rows, s_rows, h_rows in results:
        for r in b_rows:
            vals = _lm_tuple(r)
            if vals[0]:
                await pub.execute(LM_UPSERT_BUY, vals); b_up += 1
        for r in s_rows:
            vals = _lm_tuple(r)
            if vals[0]:
                await pub.execute(LM_UPSERT_SELL, vals); s_up += 1
        for r in h_rows:
            vals = _lm_ship_tuple(r)
            if vals[0]:
                await pub.execute(LM_UPSERT_SHIP, vals); h_up += 1
    await pub.commit()
    log.info(f"localmarket upserted: LM_buy={b_up} LM_sell={s_up} LM_ship={h_up}")

# ---------- loaders: PRIVATE ----------
async def load_user_inventory(session: aiohttp.ClientSession, pvt: aiosqlite.Connection, discord_id: str, username: str, apikey: str) -> int:
    rows = await fetch_csv(session, f"{PRIVATE_BASE}/csv/inventory?apikey={apikey}&username={username}")
    if not rows:
        return 0
    ts = _now_ms()
    up = 0
    await pvt.execute("BEGIN")
    for r in rows:
        tck = (r.get("Ticker") or "").strip().upper()
        if not tck:
            continue
        await pvt.execute(
            "INSERT INTO user_inventory(discord_id,username,natural_id,name,storage_type,ticker,amount,ts) VALUES(?,?,?,?,?,?,?,?) "
            "ON CONFLICT(discord_id,ticker,natural_id) DO UPDATE SET username=excluded.username, name=excluded.name, storage_type=excluded.storage_type, amount=excluded.amount, ts=excluded.ts",
            (
                discord_id,
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
    await pvt.commit()
    log.info(f"inventory[{username}] upserted={up}")
    return up

async def load_user_cxos(session: aiohttp.ClientSession, pvt: aiosqlite.Connection, discord_id: str, username: str, apikey: str) -> int:
    rows = await fetch_csv(session, f"{PRIVATE_BASE}/csv/cxos?apikey={apikey}&username={username}")
    if not rows:
        return 0
    ts = _now_ms()
    up = 0
    await pvt.execute("BEGIN")
    for r in rows:
        oid = (r.get("OrderId") or "").strip()
        if not oid:
            continue
        await pvt.execute(
            "INSERT INTO user_cxos(discord_id,username,order_id,exchange_code,order_type,material_ticker,amount,initial_amount,price_limit,currency,status,created_epoch_ms,ts) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(discord_id,order_id) DO UPDATE SET username=excluded.username, exchange_code=excluded.exchange_code, order_type=excluded.order_type, "
            "material_ticker=excluded.material_ticker, amount=excluded.amount, initial_amount=excluded.initial_amount, price_limit=excluded.price_limit, "
            "currency=excluded.currency, status=excluded.status, created_epoch_ms=excluded.created_epoch_ms, ts=excluded.ts",
            (
                discord_id,
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
    await pvt.commit()
    log.info(f"cxos[{username}] upserted={up}")
    return up

async def load_user_balances(session: aiohttp.ClientSession, pvt: aiosqlite.Connection, discord_id: str, username: str, apikey: str) -> int:
    rows = await fetch_csv(session, f"{PRIVATE_BASE}/csv/balances?apikey={apikey}&username={username}")
    if not rows:
        return 0
    ts = _now_ms()
    up = 0
    await pvt.execute("BEGIN")
    for r in rows:
        cur = (r.get("Currency") or "").strip().upper()
        if not cur:
            continue
        await pvt.execute(
            "INSERT INTO user_balances(discord_id,username,currency,amount,ts) VALUES(?,?,?,?,?) "
            "ON CONFLICT(discord_id,currency) DO UPDATE SET username=excluded.username, amount=excluded.amount, ts=excluded.ts",
            (discord_id, username, cur, _to_float(r.get("Amount")), ts),
        )
        up += 1
    await pvt.commit()
    log.info(f"balances[{username}] upserted={up}")
    return up

# ---------- main loops ----------
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
    try:
        await load_public_localmarket(session, pub)
    except Exception as e:
        log.error(f"localmarket error: {e}")
    log.info(f"public cycle took {(time.time()-t0):.2f}s")

async def load_private_all(pvt, usersdb):
    cur = await usersdb.execute(
        "SELECT discord_id, username, fio_api_key FROM users WHERE fio_api_key IS NOT NULL AND fio_api_key <> ''"
    )
    rows = await cur.fetchall()
    if not rows:
        log.info("private users=0")
        return
    totals = {"inv": 0, "cx": 0, "bal": 0}
    for discord_id, username, apikey in rows:
        if not (discord_id and username and apikey):
            continue
        try:
            totals["inv"] += await load_user_inventory(session, pvt, discord_id, username, apikey)
            totals["cx"]  += await load_user_cxos(session, pvt, discord_id, username, apikey)
            totals["bal"] += await load_user_balances(session, pvt, discord_id, username, apikey)
        except Exception as e:
            log.error(f"private[{username}] error: {e}")
    log.info(f"private totals inventory={totals['inv']} cxos={totals['cx']} balances={totals['bal']}")

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
