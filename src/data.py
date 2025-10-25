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
import re
import time
from typing import Any, Dict, List, Literal, Optional, Tuple
from collections import defaultdict

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

# --- tuning knobs (after other config imports) ---
HTTP_MAX_CONNECTIONS   = getattr(config, "HTTP_MAX_CONNECTIONS", 1000)   # hard cap
CONCURRENCY_LM         = getattr(config, "CONCURRENCY_LM", 256)
CONCURRENCY_CXPC       = getattr(config, "CONCURRENCY_CXPC", 512)
DB_BATCH_SIZE          = getattr(config, "DB_BATCH_SIZE", 500)

EXCHANGES = config.EXCHANGES
LM_EXTRA_PLANET_IDS = ["MOR", "HRT", "ANT", "BEN", "HUB", "ARC"]
MAJOR_CX = ("NC1", "CI1", "IC1", "AI1")                 
CX_CODES = ("NC1", "NC2", "CI1", "CI2", "IC1", "AI1")
VALID_CX = set(("NC1", "NC2", "CI1", "CI2", "IC1", "AI1"))
UN_CODE  = "UN"               

CHART_ALLOWED = {"MINUTE_FIVE": 7, "HOUR_TWO": 30}  # days of retention per interval

PRUNPLANNER_EXCHANGE_URL = "http://api.prunplanner.org/csv/exchange?api_key=GY0aQcLcAM4y1gSeoPWaxCi2y1Vn9VKO"  # CSV: TICKER,EXCHANGECODE,ASK,BID,AVG,SUPPLY,DEMAND,TRADED

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
  PP7 REAL,          -- NEW
  PP30 REAL,         -- NEW
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

PRICES_CHART_EXPECTED_COLS = [
  "cx","ticker","mat_id","interval","ts",
  "open","close","high","low","volume","traded"
]

PRICES_CHART_SCHEMA = """
CREATE TABLE IF NOT EXISTS prices_chart_history(
  cx       TEXT    NOT NULL,
  ticker   TEXT    NOT NULL,
  mat_id   INTEGER NOT NULL,
  interval TEXT    NOT NULL,   -- MINUTE_FIVE or HOUR_TWO
  ts       INTEGER NOT NULL,   -- DateEpochMs
  open   REAL,
  close  REAL,
  high   REAL,
  low    REAL,
  volume REAL,
  traded INTEGER,
  PRIMARY KEY(cx,ticker,interval,ts)
);
CREATE INDEX IF NOT EXISTS ix_pch_ts ON prices_chart_history(ts);
"""

# --- HTTP cache for conditional GETs (ETag / Last-Modified) ---
HTTP_CACHE_SCHEMA = """
CREATE TABLE IF NOT EXISTS http_cache(
  url TEXT PRIMARY KEY,
  etag TEXT,
  last_modified TEXT,
  fetched_ts INTEGER
);
"""

# ---------- helper: db ----------
async def ensure_private_schema(pvt: aiosqlite.Connection):
    await _ensure_table(pvt, "user_inventory", PVT_EXPECTED["user_inventory"], PVT_SCHEMA_INV)
    await _ensure_table(pvt, "user_cxos",      PVT_EXPECTED["user_cxos"],      PVT_SCHEMA_CXOS)
    await _ensure_table(pvt, "user_balances",  PVT_EXPECTED["user_balances"],  PVT_SCHEMA_BAL)

async def ensure_prices_chart_schema(pub: aiosqlite.Connection):
    await _ensure_table(pub, "prices_chart_history", PRICES_CHART_EXPECTED_COLS, PRICES_CHART_SCHEMA)

async def ensure_http_cache(pub: aiosqlite.Connection):
    await pub.executescript(HTTP_CACHE_SCHEMA); await pub.commit()

async def _http_cache_get(pub: aiosqlite.Connection, url: str) -> Tuple[Optional[str], Optional[str]]:
    row = await (await pub.execute("SELECT etag,last_modified FROM http_cache WHERE url=?", (url,))).fetchone()
    return (row[0], row[1]) if row else (None, None)

async def _http_cache_put(pub: aiosqlite.Connection, url: str, etag: Optional[str], last_modified: Optional[str]):
    await pub.execute(
        "INSERT INTO http_cache(url,etag,last_modified,fetched_ts) VALUES(?,?,?,?) "
        "ON CONFLICT(url) DO UPDATE SET etag=excluded.etag, last_modified=excluded.last_modified, fetched_ts=excluded.fetched_ts",
        (url, etag, last_modified, _now_ms())
    )

async def init_dbs():
    pub = await aiosqlite.connect(PRUN_DB_PATH)
    await pub.executescript(PUB_SCHEMA); await pub.commit()
    await ensure_lm_ship_schema(pub)
    await ensure_http_cache(pub)  # add

    pvt = await aiosqlite.connect(PVT_DB_PATH)
    await ensure_private_schema(pvt)

    users = await aiosqlite.connect(USER_DB_PATH)
    return pub, pvt, users


# ---------- helper: pp7d, pp30d ----------
async def ensure_prices_pp_columns(pub: aiosqlite.Connection):
    cols: List[str] = []
    async with pub.execute("PRAGMA table_info(prices)") as cur:
        async for _cid, name, *_ in cur:
            cols.append((name or "").lower())
    # migrate existing DBs
    if "pp7" not in cols:
        await pub.execute("ALTER TABLE prices ADD COLUMN PP7 REAL")
    if "pp30" not in cols:
        await pub.execute("ALTER TABLE prices ADD COLUMN PP30 REAL")
    await pub.commit()

async def _pp_means(pub: aiosqlite.Connection, cx: str, ticker: str, now_ms: int) -> Tuple[Optional[float], Optional[float]]:
    """Return (PP7, PP30) as mean of mid = (bid+ask)/2 over the last 7/30 days."""
    cutoff30 = now_ms - 30*24*3600*1000
    cutoff7  = now_ms - 7*24*3600*1000
    rows = await (await pub.execute(
        "SELECT ts,bid,ask FROM price_history WHERE cx=? AND ticker=? AND ts>=? ORDER BY ts ASC",
        (cx, ticker, cutoff30)
    )).fetchall()
    mids30: List[Tuple[int, float]] = []
    for ts, b, a in rows:
        if b is None or a is None:
            continue
        mids30.append((int(ts), (float(b)+float(a))/2.0))
    if not mids30:
        return None, None
    pp30_vals = [m for _ts, m in mids30]
    pp7_vals  = [m for ts, m in mids30 if ts >= cutoff7]
    def _mean(v: List[float]) -> Optional[float]:
        return (sum(v)/len(v)) if v else None
    return _mean(pp7_vals), _mean(pp30_vals)

# ---------- helper: fetch json data ----------
async def fetch_json(session: aiohttp.ClientSession, url: str) -> Optional[Any]:
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            if resp.status != 200:
                log.warning(f"GET {url} -> {resp.status}")
                return None
            if resp.status == 200:
                log.info(f"GET {url} -> {resp.status}")
            return await resp.json(content_type=None)
    except Exception as e:
        log.error(f"fetch_json error url={url} err={e}")
        return None

# ---------- helper: materials maps ----------
async def material_id_map(pub: aiosqlite.Connection) -> Dict[str, int]:
    rows = await (await pub.execute("SELECT ticker FROM materials ORDER BY ticker ASC")).fetchall()
    return {t: i for i, (t,) in enumerate(rows)}

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

async def load_public_prices_prunplanner(session: aiohttp.ClientSession, pub: aiosqlite.Connection):
    await ensure_prices_pp_columns(pub)

    rows = await fetch_csv(session, PRUNPLANNER_EXCHANGE_URL)
    if not rows:
        return

    ts = _now_ms()
    # collect per-ticker quotes seen this tick
    # by_ticker[ticker][cx] = (bid, ask)
    by_ticker: Dict[str, Dict[str, Tuple[Optional[float], Optional[float]]]] = defaultdict(dict)

    await pub.execute("BEGIN")

    # 1) upsert raw CX quotes (NC1, NC2, CI1, CI2, IC1, AI1) and build cache
    for r in rows:
        ticker = _to_str(r.get("TICKER"))
        cx     = _to_str(r.get("EXCHANGECODE"))
        ask    = _to_float(r.get("ASK"))
        bid    = _to_float(r.get("BID"))
        if not ticker or not cx:
            continue
        if cx not in VALID_CX:
            continue
        if ask is None and bid is None:
            continue

        by_ticker[ticker][cx] = (bid, ask)

        # history tick for the concrete CX
        await pub.execute(
            "INSERT OR IGNORE INTO price_history(cx,ticker,bid,ask,ts) VALUES(?,?,?,?,?)",
            (cx, ticker, bid, ask, ts)
        )

        # rolling PP7 / PP30 for the concrete CX
        pp7, pp30 = await _pp_means(pub, cx, ticker, ts)

        # snapshot
        await pub.execute(
            "INSERT INTO prices(cx,ticker,best_bid,best_ask,PP7,PP30,ts) VALUES(?,?,?,?,?,?,?) "
            "ON CONFLICT(cx,ticker) DO UPDATE SET "
            "best_bid=excluded.best_bid, best_ask=excluded.best_ask, "
            "PP7=excluded.PP7, PP30=excluded.PP30, ts=excluded.ts",
            (cx, ticker, bid, ask, pp7, pp30, ts)
        )

    # 2) synthesize UN = average of the four majors (XX1)
    for ticker, cxmap in by_ticker.items():
        bids = []
        asks = []
        for m in MAJOR_CX:
            pair = cxmap.get(m)
            if not pair:
                continue
            b, a = pair
            if b is not None:
                bids.append(b)
            if a is not None:
                asks.append(a)

        un_bid = sum(bids) / len(bids) if bids else None
        un_ask = sum(asks) / len(asks) if asks else None
        if un_bid is None and un_ask is None:
            continue

        # history tick for UN
        await pub.execute(
            "INSERT OR IGNORE INTO price_history(cx,ticker,bid,ask,ts) VALUES(?,?,?,?,?)",
            (UN_CODE, ticker, un_bid, un_ask, ts)
        )

        # PP7 / PP30 for UN are computed from UN’s own history
        pp7_un, pp30_un = await _pp_means(pub, UN_CODE, ticker, ts)

        # snapshot for UN
        await pub.execute(
            "INSERT INTO prices(cx,ticker,best_bid,best_ask,PP7,PP30,ts) VALUES(?,?,?,?,?,?,?) "
            "ON CONFLICT(cx,ticker) DO UPDATE SET "
            "best_bid=excluded.best_bid, best_ask=excluded.best_ask, "
            "PP7=excluded.PP7, PP30=excluded.PP30, ts=excluded.ts",
            (UN_CODE, ticker, un_bid, un_ask, pp7_un, pp30_un, ts)
        )

    await pub.commit()

    # retention
    cutoff = int((time.time() - RETENTION_SEC) * 1000)
    await pub.execute("DELETE FROM price_history WHERE ts < ?", (cutoff,))
    await pub.commit()


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

async def load_public_prices_chart_history(session: aiohttp.ClientSession, pub: aiosqlite.Connection):
    await ensure_prices_chart_schema(pub)

    id_map = await material_id_map(pub)
    if not id_map:
        log.info("prices_chart_history: no materials loaded yet")
        return

    sem = asyncio.Semaphore(CONCURRENCY_CXPC)
    now_ms = _now_ms()

    async def _fetch_and_store(ticker: str, mat_id: int, cx: str):
        url = f"https://rest.fnar.net/exchange/cxpc/{ticker}.{cx}"
        # conditional request if we have cache
        etag, last_mod = await _http_cache_get(pub, url)
        headers = {}
        if etag: headers["If-None-Match"] = etag
        if last_mod: headers["If-Modified-Since"] = last_mod
        try:
            async with session.get(url, headers=headers) as resp:
                if resp.status == 304:
                    return 0
                if resp.status != 200:
                    log.warning(f"cxpc {url} -> {resp.status}")
                    return 0
                # store cache headers for next cycle
                await _http_cache_put(pub, url, resp.headers.get("ETag"), resp.headers.get("Last-Modified"))
                data = await resp.json(content_type=None)
        except Exception as e:
            log.error(f"cxpc GET err {url}: {e}")
            return 0

        if not isinstance(data, list):
            return 0

        rows = []
        for d in data:
            interval = _to_str(d.get("Interval"))
            if interval not in CHART_ALLOWED:
                continue
            ts = _to_int(d.get("DateEpochMs"))
            if ts is None:
                continue
            rows.append((
                cx, ticker, mat_id, interval, ts,
                _to_float(d.get("Open")),
                _to_float(d.get("Close")),
                _to_float(d.get("High")),
                _to_float(d.get("Low")),
                _to_float(d.get("Volume")),
                _to_int(d.get("Traded")),
            ))
        if not rows:
            return 0

        # single executemany per url
        await pub.executemany(
            "INSERT INTO prices_chart_history(cx,ticker,mat_id,interval,ts,open,close,high,low,volume,traded) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(cx,ticker,interval,ts) DO UPDATE SET "
            "open=excluded.open, close=excluded.close, high=excluded.high, low=excluded.low, "
            "volume=excluded.volume, traded=excluded.traded",
            rows
        )
        return len(rows)

    async def _task(ticker: str, mat_id: int, cx: str):
        async with sem:
            try:
                return await _fetch_and_store(ticker, mat_id, cx)
            except Exception as e:
                log.error(f"cxpc load err {ticker}.{cx}: {e}")
                return 0

    tasks = [asyncio.create_task(_task(tk, mid, cx)) for tk, mid in id_map.items() for cx in CX_CODES]
    upserts = 0
    await pub.execute("BEGIN")
    for t in asyncio.as_completed(tasks):
        upserts += await t
    await pub.commit()
    log.info(f"prices_chart_history upserts={upserts}")

    cutoff_5m = now_ms - 7*24*3600*1000
    cutoff_2h = now_ms - 30*24*3600*1000
    await pub.execute("DELETE FROM prices_chart_history WHERE interval='MINUTE_FIVE' AND ts<?", (cutoff_5m,))
    await pub.execute("DELETE FROM prices_chart_history WHERE interval='HOUR_TWO' AND ts<?", (cutoff_2h,))
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

    sem = asyncio.Semaphore(CONCURRENCY_LM)

    async def _fetch(pid: str):
        async with sem:
            # 3 concurrent GETs per planet
            buy_c = asyncio.create_task(_fetch_lm_kind(session, pid, "buy"))
            sell_c = asyncio.create_task(_fetch_lm_kind(session, pid, "sell"))
            ship_c = asyncio.create_task(_fetch_lm_kind(session, pid, "ship"))
            br, sr, hr = await asyncio.gather(buy_c, sell_c, ship_c, return_exceptions=False)
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

    # batch upserts
    buy_buf, sell_buf, ship_buf = [], [], []
    for _, b_rows, s_rows, h_rows in results:
        for r in b_rows:
            vals = _lm_tuple(r)
            if vals[0]: buy_buf.append(vals)
        for r in s_rows:
            vals = _lm_tuple(r)
            if vals[0]: sell_buf.append(vals)
        for r in h_rows:
            vals = _lm_ship_tuple(r)
            if vals[0]: ship_buf.append(vals)

    await pub.execute("BEGIN")
    for i in range(0, len(buy_buf), DB_BATCH_SIZE):
        await pub.executemany(LM_UPSERT_BUY, buy_buf[i:i+DB_BATCH_SIZE])
    for i in range(0, len(sell_buf), DB_BATCH_SIZE):
        await pub.executemany(LM_UPSERT_SELL, sell_buf[i:i+DB_BATCH_SIZE])
    for i in range(0, len(ship_buf), DB_BATCH_SIZE):
        await pub.executemany(LM_UPSERT_SHIP, ship_buf[i:i+DB_BATCH_SIZE])
    await pub.commit()
    log.info(f"localmarket upserted: LM_buy={len(buy_buf)} LM_sell={len(sell_buf)} LM_ship={len(ship_buf)}")

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
        # REPLACE the old /csv/prices loader with PrunPlanner exchange CSV
        await load_public_prices_prunplanner(session, pub)
    except Exception as e:
        log.error(f"prices(prunplanner) error: {e}")
    try:
        log.info("starting pries chart history pull")
        await load_public_prices_chart_history(session, pub)
    except Exception as e:
        log.error(f"prices_chart_history error: {e}")
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
    # high-conn pool + keepalive + DNS cache
    conn = aiohttp.TCPConnector(limit=HTTP_MAX_CONNECTIONS, limit_per_host=0, ttl_dns_cache=300, enable_cleanup_closed=True, ssl=False)
    timeout = aiohttp.ClientTimeout(total=60, connect=10, sock_read=40)
    session = aiohttp.ClientSession(connector=conn, timeout=timeout, headers={"Connection": "keep-alive"})
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
