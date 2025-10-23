#!/usr/bin/env python3
"""
PrUn Database Initializer

Initializes all database tables and schemas for the PrUn system.
This script should be run once to set up the database structure.

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

# Database schemas
PUBLIC_SCHEMA = """
PRAGMA journal_mode=WAL;

-- Public market data tables
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

CREATE TABLE IF NOT EXISTS books(
  cx TEXT NOT NULL,
  ticker TEXT NOT NULL,
  side TEXT NOT NULL CHECK(side IN('bid','ask')),
  price REAL NOT NULL,
  qty REAL NOT NULL,
  ts INTEGER NOT NULL,
  PRIMARY KEY(cx, ticker, side, price)
);

CREATE INDEX IF NOT EXISTS ix_price_history_ts ON price_history(ts);
CREATE INDEX IF NOT EXISTS ix_books_cx_ticker ON books(cx, ticker);
CREATE INDEX IF NOT EXISTS ix_books_side_price ON books(side, price);
"""

PRIVATE_SCHEMA = """
PRAGMA journal_mode=WAL;

-- User private data tables
CREATE TABLE IF NOT EXISTS user_inventory(
  discord_id TEXT NOT NULL,
  username TEXT NOT NULL,
  natural_id TEXT NOT NULL,
  name TEXT,
  storage_type TEXT,
  ticker TEXT NOT NULL,
  amount REAL NOT NULL,
  ts INTEGER NOT NULL,
  PRIMARY KEY(discord_id, natural_id, ticker)
);

CREATE TABLE IF NOT EXISTS user_cxos(
  discord_id TEXT NOT NULL,
  username TEXT NOT NULL,
  order_id TEXT NOT NULL,
  exchange_code TEXT NOT NULL,
  order_type TEXT NOT NULL,
  material_ticker TEXT NOT NULL,
  amount REAL NOT NULL,
  initial_amount REAL,
  limit_price REAL,
  currency TEXT,
  status TEXT,
  created_epoch_ms INTEGER,
  ts INTEGER NOT NULL,
  PRIMARY KEY(discord_id, order_id)
);

CREATE TABLE IF NOT EXISTS user_balances(
  discord_id TEXT NOT NULL,
  username TEXT NOT NULL,
  currency TEXT NOT NULL,
  amount REAL NOT NULL,
  ts INTEGER NOT NULL,
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
  ticker TEXT NOT NULL,
  exchange TEXT NOT NULL DEFAULT '',
  PRIMARY KEY(guild_id, ticker, exchange)
);

CREATE TABLE IF NOT EXISTS user_watchlist(
  guild_id INTEGER NOT NULL,
  user_id INTEGER NOT NULL,
  ticker TEXT NOT NULL,
  exchange TEXT NOT NULL DEFAULT '',
  PRIMARY KEY(guild_id, user_id, ticker, exchange)
);

CREATE TABLE IF NOT EXISTS user_meta(
  guild_id INTEGER NOT NULL,
  user_id INTEGER NOT NULL,
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

async def init_database(db_path: str, schema: str, db_name: str) -> bool:
    """
    Initialize a database with the given schema.
    
    Args:
        db_path: Path to the database file
        schema: SQL schema to execute
        db_name: Name of the database for logging
        
    Returns:
        True if successful, False otherwise
    """
    try:
        log.info(f"Initializing {db_name} database: {db_path}")
        
        # Ensure directory exists
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
    """Check if database file exists and has tables."""
    try:
        if not os.path.exists(db_path):
            return False
            
        async with aiosqlite.connect(db_path) as conn:
            cursor = await conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
            tables = await cursor.fetchall()
            return len(tables) > 0
            
    except Exception:
        return False

async def main():
    """Initialize all databases."""
    log.info("Starting PrUn database initialization")
    
    # Database configurations
    databases = [
        {
            "path": config.DB_PATH,
            "schema": PUBLIC_SCHEMA,
            "name": "Public Market Data (prun.db)"
        },
        {
            "path": config.PRIVATE_DB,
            "schema": PRIVATE_SCHEMA,
            "name": "Private User Data (prun-private.db)"
        },
        {
            "path": config.USER_DB,
            "schema": USER_SCHEMA,
            "name": "User Credentials (user.db)"
        },
        {
            "path": config.BOT_DB,
            "schema": BOT_SCHEMA,
            "name": "Bot State (bot.db)"
        }
    ]
    
    # Initialize each database
    success_count = 0
    total_count = len(databases)
    
    for db_config in databases:
        db_path = db_config["path"]
        schema = db_config["schema"]
        db_name = db_config["name"]
        
        # Check if database already exists
        if await check_database_exists(db_path):
            log.info(f"Database {db_name} already exists, skipping")
            success_count += 1
            continue
            
        # Initialize database
        if await init_database(db_path, schema, db_name):
            success_count += 1
        else:
            log.error(f"Failed to initialize {db_name}")
    
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
    
    # Summary
    if success_count == total_count:
        log.info(f"✅ Successfully initialized all {total_count} databases")
        print(f"✅ Database initialization completed successfully!")
        print(f"📁 Databases created in: {config.DATABASE_DIR}")
        print(f"📝 Logs written to: {config.LOG_DIR}")
        return True
    else:
        log.error(f"❌ Failed to initialize {total_count - success_count} out of {total_count} databases")
        print(f"❌ Database initialization failed!")
        print(f"📝 Check logs in: {config.LOG_DIR}/init.log")
        return False

def print_usage():
    """Print usage information."""
    print("PrUn Database Initializer")
    print("=" * 40)
    print("This script initializes all database tables for the PrUn system.")
    print()
    print("Usage:")
    print("  python src/init.py")
    print()
    print("What it does:")
    print("  - Creates database/ folder if it doesn't exist")
    print("  - Initializes prun.db (public market data)")
    print("  - Initializes prun-private.db (user private data)")
    print("  - Initializes user.db (user credentials)")
    print("  - Initializes bot.db (bot state)")
    print("  - Creates all necessary tables and indexes")
    print()
    print("Requirements:")
    print("  - Python 3.10+")
    print("  - aiosqlite package")
    print("  - Valid .env configuration")
    print()

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
