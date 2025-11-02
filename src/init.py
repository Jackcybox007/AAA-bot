#!/usr/bin/env python3
"""
PrUn Database Initializer

Initializes all database tables and schemas for the PrUn system.
Run once to set up the database structure.

Usage:
    python src/init.py
"""

# Standard library imports
import asyncio
import logging
import os
import sys
from pathlib import Path

# Third-party imports
import aiosqlite

# Local imports
from config import config

# Setup logging
log = config.setup_logging("init")

# =========================
# Schemas
# =========================
PUBLIC_SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS prices(
  cx        TEXT NOT NULL,
  ticker    TEXT NOT NULL,
  best_bid  REAL,
  best_ask  REAL,
  PP7       REAL,
  PP30      REAL,
  ts        INTEGER NOT NULL,
  PRIMARY KEY(cx, ticker)
);

CREATE TABLE IF NOT EXISTS price_history(
  cx     TEXT NOT NULL,
  ticker TEXT NOT NULL,
  bid    REAL,
  ask    REAL,
  ts     INTEGER NOT NULL,
  PRIMARY KEY(cx, ticker, ts)
);

CREATE TABLE IF NOT EXISTS prices_chart_history(
  cx       TEXT NOT NULL,
  ticker   TEXT NOT NULL,
  interval INTEGER NOT NULL,
  ts       INTEGER NOT NULL,
  open     REAL,
  close    REAL,
  volume   REAL,
  traded   INTEGER,
  PRIMARY KEY(cx,ticker,interval,ts)
);

CREATE TABLE IF NOT EXISTS materials(
  ticker   TEXT PRIMARY KEY,
  name     TEXT,
  category TEXT,
  weight   REAL,
  volume   REAL
);

CREATE TABLE IF NOT EXISTS books(
  cx           TEXT NOT NULL,
  ticker       TEXT NOT NULL,
  side         TEXT NOT NULL CHECK(side IN('bid','ask')),
  price        REAL NOT NULL,
  qty          REAL NOT NULL,
  level        INTEGER,
  company_id   TEXT,
  company_name TEXT,
  company_code TEXT,
  ts           INTEGER NOT NULL,
  PRIMARY KEY(cx, ticker, side, price)
);

CREATE INDEX IF NOT EXISTS ix_books_cx_ticker ON books(cx, ticker);
CREATE INDEX IF NOT EXISTS ix_books_side_price ON books(side, price);
CREATE INDEX IF NOT EXISTS ix_books_ts ON books(ts);

CREATE TABLE IF NOT EXISTS http_cache(
  url TEXT PRIMARY KEY,
  ts  INTEGER NOT NULL,
  body BLOB
);

CREATE TABLE IF NOT EXISTS market_snapshot(
  ts_ms   INTEGER NOT NULL,
  ts_iso  TEXT    NOT NULL,
  cx      TEXT    NOT NULL,
  ticker  TEXT    NOT NULL,
  pb      REAL,
  pa      REAL,
  spread  REAL,
  mid     REAL,
  PP7     REAL,
  PP30    REAL,
  dev7    REAL,
  dev30   REAL,
  z7      REAL
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_market_snapshot_cx_ticker ON market_snapshot(cx, ticker);
CREATE INDEX IF NOT EXISTS idx_snapshot_time ON market_snapshot(ts_ms);

CREATE TABLE IF NOT EXISTS market_stats(
  ts_ms    INTEGER NOT NULL,
  category TEXT    NOT NULL,
  rank     INTEGER NOT NULL,
  cx       TEXT,
  ticker   TEXT,
  value    REAL,
  PRIMARY KEY(ts_ms, category, rank)
);
"""

PRIVATE_SCHEMA = """
PRAGMA journal_mode=WAL;

-- User private data tables
CREATE TABLE IF NOT EXISTS user_inventory(
  discord_id   TEXT NOT NULL,
  username     TEXT NOT NULL,
  natural_id   TEXT NOT NULL,
  name         TEXT,
  storage_type TEXT,
  ticker       TEXT NOT NULL,
  amount       REAL NOT NULL,
  ts           INTEGER NOT NULL,
  PRIMARY KEY(discord_id, natural_id, ticker)
);

CREATE TABLE IF NOT EXISTS user_cxos(
  discord_id       TEXT NOT NULL,
  username         TEXT NOT NULL,
  order_id         TEXT NOT NULL,
  exchange_code    TEXT NOT NULL,
  order_type       TEXT NOT NULL,
  material_ticker  TEXT NOT NULL,
  amount           REAL NOT NULL,
  initial_amount   REAL,
  limit_price      REAL,
  currency         TEXT,
  status           TEXT,
  created_epoch_ms INTEGER,
  ts               INTEGER NOT NULL,
  PRIMARY KEY(discord_id, order_id)
);

CREATE TABLE IF NOT EXISTS user_balances(
  discord_id TEXT NOT NULL,
  username   TEXT NOT NULL,
  currency   TEXT NOT NULL,
  amount     REAL NOT NULL,
  ts         INTEGER NOT NULL,
  PRIMARY KEY(discord_id, currency)
);
"""

USER_SCHEMA = """
PRAGMA journal_mode=WAL;

-- User credentials table
CREATE TABLE IF NOT EXISTS users(
  discord_id TEXT PRIMARY KEY,
  fio_api_key TEXT NOT NULL,
  username TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
"""

BOT_SCHEMA = """
PRAGMA journal_mode=WAL;

-- Bot state tables
CREATE TABLE IF NOT EXISTS server_watchlist(
  guild_id INTEGER NOT NULL,
  ticker   TEXT NOT NULL,
  exchange TEXT NOT NULL DEFAULT '',
  PRIMARY KEY(guild_id, ticker, exchange)
);

CREATE TABLE IF NOT EXISTS user_watchlist(
  guild_id INTEGER NOT NULL,
  user_id  INTEGER NOT NULL,
  ticker   TEXT NOT NULL,
  exchange TEXT NOT NULL DEFAULT '',
  PRIMARY KEY(guild_id, user_id, ticker, exchange)
);

CREATE TABLE IF NOT EXISTS user_meta(
  guild_id INTEGER NOT NULL,
  user_id  INTEGER NOT NULL,
  private_channel_id INTEGER,
  PRIMARY KEY(guild_id, user_id)
);
"""

MCP_SCHEMA = """
PRAGMA journal_mode=WAL;

-- MCP server helper tables
CREATE TABLE IF NOT EXISTS assets_holdings(
  id INTEGER PRIMARY KEY,
  ticker TEXT NOT NULL,
  base TEXT,
  qty REAL NOT NULL DEFAULT 0,
  avg_cost REAL NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS assets_txn(
  id INTEGER PRIMARY KEY,
  ticker TEXT NOT NULL,
  base TEXT,
  qty REAL NOT NULL,
  price REAL NOT NULL,
  at TEXT NOT NULL
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

CREATE INDEX IF NOT EXISTS idx_assets_holdings_tb ON assets_holdings(ticker,base);
CREATE INDEX IF NOT EXISTS idx_planned_orders ON planned_orders(status,cx,ticker,side);
"""

# =========================
# Helpers
# =========================
async def init_database(db_path: str, schema: str, db_name: str) -> bool:
    try:
        log.info(f"Initializing {db_name} database: {db_path}")
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        async with aiosqlite.connect(db_path) as conn:
            await conn.executescript(schema)
            await conn.commit()
        log.info(f"Successfully initialized {db_name} database")
        return True
    except Exception as e:
        log.error(f"Failed to initialize {db_name} database: {e}")
        return False

async def check_database_exists(db_path: str) -> bool:
    try:
        if not os.path.exists(db_path):
            return False
        async with aiosqlite.connect(db_path) as conn:
            cursor = await conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = await cursor.fetchall()
            return len(tables) > 0
    except Exception:
        return False

async def _column_exists(con: aiosqlite.Connection, table: str, column: str) -> bool:
    try:
        rows = con.execute(f"PRAGMA table_info({table})").fetchall()
        return any(r[1] == column for r in rows)
    except Exception:
        return False

async def migrate_books_schema(con: aiosqlite.Connection):
    cols = {
        "level": "INTEGER",
        "company_id": "TEXT",
        "company_name": "TEXT",
        "company_code": "TEXT"
    }
    for c, typ in cols.items():
        try:
            if not _column_exists(con, "books", c):
                con.execute(f"ALTER TABLE books ADD COLUMN {c} {typ}")
        except Exception:
            pass
    con.executescript("""
CREATE TABLE IF NOT EXISTS books_hist(
  cx           TEXT NOT NULL,
  ticker       TEXT NOT NULL,
  side         TEXT NOT NULL CHECK(side IN('bid','ask')),
  price        REAL NOT NULL,
  qty          REAL NOT NULL,
  level        INTEGER,
  company_id   TEXT,
  company_name TEXT,
  company_code TEXT,
  ts           INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_books_hist_cts   ON books_hist(cx,ticker,side,ts);
CREATE INDEX IF NOT EXISTS ix_books_hist_lvl   ON books_hist(cx,ticker,side,level,ts);
CREATE INDEX IF NOT EXISTS ix_books_hist_price ON books_hist(cx,ticker,side,price,ts);
""")

# =========================
# Main
# =========================
async def main():
    log.info("Starting PrUn database initialization")

    databases = [
        {"path": config.DB_PATH,      "schema": PUBLIC_SCHEMA,  "name": "Public Market Data (prun.db)"},
        {"path": config.PRIVATE_DB,   "schema": PRIVATE_SCHEMA, "name": "Private User Data (prun-private.db)"},
        {"path": config.USER_DB,      "schema": USER_SCHEMA,    "name": "User Credentials (user.db)"},
        {"path": config.BOT_DB,       "schema": BOT_SCHEMA,     "name": "Bot State (bot.db)"},
    ]

    success_count = 0
    total_count = len(databases)

    for db in databases:
        if await check_database_exists(db["path"]):
            log.info(f"Database {db['name']} already exists, skipping")
            success_count += 1
            continue
        if await init_database(db["path"], db["schema"], db["name"]):
            success_count += 1
        else:
            log.error(f"Failed to initialize {db['name']}")

    # Initialize MCP helper tables in public database
    if success_count > 0:
        log.info("Initializing MCP helper tables in public database")
        try:
            async with aiosqlite.connect(config.DB_PATH) as conn:
                await conn.executescript(MCP_SCHEMA)
                await conn.commit()
            log.info("Successfully initialized MCP helper tables")
        except Exception as e:
            log.error(f"Failed to initialize MCP helper tables: {e}")

    if db["path"] == config.DB_PATH:
      try:
          con = aiosqlite.connect(config.DB_PATH)
          migrate_books_schema(con)
          con.commit(); con.close()
          log.info("Post-migration on books done")
      except Exception as e:
          log.error(f"Post-migration failed: {e}")

    if success_count == total_count:
        log.info(f"Successfully initialized all {total_count} databases")
        print("✅ Database initialization completed successfully!")
        print(f"📁 Databases created in: {config.DATABASE_DIR}")
        print(f"📝 Logs written to: {config.LOG_DIR}")
        return True
    else:
        log.error(f"Failed to initialize {total_count - success_count} out of {total_count} databases")
        print("❌ Database initialization failed!")
        print(f"📝 Check logs in: {config.LOG_DIR}/init.log")
        return False

def print_usage():
    print("PrUn Database Initializer")
    print("=" * 40)
    print("Initializes all database tables for the PrUn system.\n")
    print("Usage:\n  python src/init.py\n")
    print("Creates:")
    print("  - prun.db (public market data)")
    print("  - prun-private.db (user private data)")
    print("  - user.db (user credentials)")
    print("  - bot.db (bot state)")
    print("  - all necessary tables and indexes\n")
    print("Requirements:")
    print("  - Python 3.10+")
    print("  - aiosqlite")
    print("  - valid .env configuration")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] in ["-h", "--help", "help"]:
        print_usage()
        sys.exit(0)
    try:
        success = asyncio.run(main())
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        log.info("Initialization interrupted by user")
        print("\n❌ Initialization interrupted by user")
        sys.exit(1)
    except Exception as e:
        log.error(f"Unexpected error during initialization: {e}")
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)
