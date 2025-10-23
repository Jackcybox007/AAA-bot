# PrUn - Prosperous Universe Market Monitoring System

## Overview

PrUn is a comprehensive market monitoring and analysis system for Prosperous Universe, consisting of multiple integrated components that work together to provide real-time market data, Discord bot functionality, and advanced market analysis tools.

## Project Structure

```
PrUn/
├── src/                    # Source code
│   ├── bot.py             # Discord bot
│   ├── data.py            # Data fetcher
│   ├── prun-mcp.py        # MCP server
│   ├── dump.py            # Database exporter
│   ├── init.py            # Database initializer
│   └── config.py          # Centralized configuration
├── database/              # SQLite databases
│   ├── prun.db           # Public market data
│   ├── prun-private.db   # User private data
│   ├── user.db           # User credentials
│   └── bot.db            # Bot state (watchlists, channels)
├── log/                   # Log files
│   ├── bot.log           # Bot operations
│   ├── data.log          # Data fetcher operations
│   ├── mcp.log           # MCP server operations
│   └── db.log            # Database operations
├── state/                 # State files (created by MCP server)
├── .env copy             # Configuration template
├── requirements.txt       # Python dependencies
└── README.md             # This file
```

## Quick Start

### 1. Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Configure environment
cp ".env copy" .env
# Edit .env with your actual values
```

### 2. Configuration

Edit `.env` with your credentials:

```bash
# Required
DISCORD_TOKEN=your_discord_bot_token_here
TARGET_GUILD_ID=your_discord_guild_id_here
OWNER_ID=your_discord_user_id_here

# FNAR API (for private data)
FNAR_USERNAME=your_username_here
FNAR_CSV_API_KEY=your_api_key_here
```

### 3. Initialize Databases

```bash
# Initialize all database tables (run once)
python src/init.py
```

### 4. Running Components

```bash
# Data fetcher (polls FNAR API)
python src/data.py

# Discord bot
python src/bot.py

# MCP server (market analysis tools)
python src/prun-mcp.py

# Database exporter
python src/dump.py
```

## Component Details

### `src/data.py` - Data Fetcher

**Purpose**: Polls FNAR API and stores data in SQLite databases.

**Features**:
- Public data: `/csv/materials` and `/csv/prices` → `database/prun.db`
- Private data: User inventory, orders, balances → `database/prun-private.db`
- Automatic retention pruning (45 days for price history)
- Comprehensive logging to `log/data.log`

**Database Schema**:
- `prices`: Latest market prices
- `price_history`: Historical data (5-min buckets, 45-day retention)
- `materials`: Material information
- `user_inventory`, `user_cxos`, `user_balances`: Private user data

### `src/bot.py` - Discord Bot

**Purpose**: Guild-scoped Discord bot for market monitoring and user interaction.

**Features**:
- Slash commands for watchlist management
- Private channel creation and management
- Market snapshots and reporting
- N8N webhook integration for AI chat
- User API key registration

**Key Commands**:
- `/register <api_key>` - Register FNAR API key
- `/watchlistadd <ticker>` - Add to server watchlist
- `/privateadd <ticker>` - Add to personal watchlist
- `/report_now` - Generate immediate market report

**Logging**: All operations logged to `log/bot.log`

### `src/prun-mcp.py` - MCP Server

**Purpose**: HTTP server providing market analysis tools via Model Context Protocol.

**Features**:
- Market analysis: prices, spreads, depth, arbitrage detection
- Asset management: holdings, transactions, P&L tracking
- Historical data analysis and statistics
- Order planning and recommendations
- Private data access (inventory, orders, balances)

**Access**: HTTP endpoint at `http://localhost:8080/mcp`

**Logging**: All operations logged to `log/mcp.log`

### `src/dump.py` - Database Exporter

**Purpose**: Export SQLite databases to CSV/JSONL formats for analysis.

**Features**:
- Exports all databases with schema
- Configurable output options
- Row count summaries
- Multiple export formats (CSV, JSONL)

**Usage**:
```bash
python src/dump.py --out export_dir
```

### `src/init.py` - Database Initializer

**Purpose**: Initialize all database tables and schemas for the PrUn system.

**Features**:
- Creates all necessary database tables
- Sets up proper indexes for performance
- Initializes WAL mode for durability
- Checks for existing databases to avoid conflicts
- Comprehensive logging of initialization process

**Usage**:
```bash
python src/init.py
```

**What it creates**:
- `database/prun.db` - Public market data tables
- `database/prun-private.db` - User private data tables
- `database/user.db` - User credentials table
- `database/bot.db` - Bot state tables
- MCP helper tables in public database

### `src/config.py` - Configuration Management

**Purpose**: Centralized configuration for all components.

**Features**:
- Environment variable management
- Path configuration (databases, logs, state)
- Configuration validation
- Centralized logging setup

## Database Structure

### `database/prun.db` (Public Data)
- `prices`: Latest market prices by exchange
- `price_history`: Historical price data
- `materials`: Material information and properties

### `database/prun-private.db` (User Data)
- `user_inventory`: User inventory data
- `user_cxos`: User orders and transactions
- `user_balances`: User currency balances

### `database/user.db` (Credentials)
- `users`: Discord ID to FNAR API key mapping

### `database/bot.db` (Bot State)
- `server_watchlist`: Server-wide watchlists
- `user_watchlist`: Per-user watchlists
- `user_meta`: Private channel mappings

## Logging System

All components use centralized logging with individual log files:

- `log/bot.log`: Discord bot operations
- `log/data.log`: Data fetcher operations
- `log/mcp.log`: MCP server operations
- `log/db.log`: Database operations from all components

## Data Flow

1. **`data.py`** polls FNAR API and stores in `database/` folder
2. **`bot.py`** reads from databases for market reports and user interaction
3. **`prun-mcp.py`** provides analysis tools over HTTP
4. **`dump.py`** exports data for external analysis

## Configuration

All configuration is managed through `src/config.py` and environment variables in `.env`:

- **Database paths**: Automatically managed in `database/` folder
- **Log paths**: Automatically managed in `log/` folder
- **API credentials**: Set in `.env` file
- **Discord settings**: Bot token, guild ID, channel IDs

## Troubleshooting

### Common Issues

1. **Database errors**: Check `log/db.log` for database operation errors
2. **Bot connection**: Check `log/bot.log` for Discord connection issues
3. **Data fetching**: Check `log/data.log` for API polling errors
4. **MCP server**: Check `log/mcp.log` for server operation errors

### Log Analysis

```bash
# Monitor all logs
tail -f log/*.log

# Check specific component
tail -f log/bot.log
```

## Security

- Never commit `.env` file to version control
- Use strong, unique API keys
- Restrict database file permissions
- Regular security updates

## Maintenance

### Regular Tasks

1. **Monitor logs** in `log/` folder
2. **Database backups** from `database/` folder
3. **Update dependencies** from `requirements.txt`
4. **Clean old logs** periodically

### Backup Strategy

```bash
# Backup databases
cp -r database/ backup/database_$(date +%Y%m%d)

# Backup configuration
cp .env backup/env_$(date +%Y%m%d)
```

## Support

For issues:
1. Check relevant log files in `log/` folder
2. Verify configuration in `.env`
3. Test individual components
4. Review this documentation

## License

See LICENSE file for details.

