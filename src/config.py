#!/usr/bin/env python3
"""
PrUn Configuration Module

Centralized configuration management for all PrUn components.
Provides consistent environment variable handling and validation.
"""

import os
import logging
from pathlib import Path
from typing import List, Set, Optional
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class PrUnConfig:
    """Centralized configuration for PrUn components."""
    
    def __init__(self):
        """Initialize configuration from environment variables."""
        self._load_database_config()
        self._load_api_config()
        self._load_discord_config()
        self._load_mcp_config()
        self._load_system_config()
    
    def _load_database_config(self):
        """Load database-related configuration."""
        # Create database directory
        self.DATABASE_DIR = os.path.abspath("./database")
        os.makedirs(self.DATABASE_DIR, exist_ok=True)
        
        # Database paths in database/ folder
        self.DB_PATH = os.path.join(self.DATABASE_DIR, "prun.db")
        self.PRIVATE_DB = os.path.join(self.DATABASE_DIR, "prun-private.db")
        self.USER_DB = os.path.join(self.DATABASE_DIR, "user.db")
        self.BOT_DB = os.path.join(self.DATABASE_DIR, "bot.db")  # Changed from Discord.db
    
    def _load_api_config(self):
        """Load API-related configuration."""
        self.FNAR_API_BASE = os.getenv("FNAR_API_BASE", "https://rest.fnar.net")
        self.PRIVATE_BASE = os.getenv("PRIVATE_BASE", "https://rest.fnar.net")
        self.FNAR_USERNAME = os.getenv("FNAR_USERNAME", "")
        self.FNAR_PASSWORD = os.getenv("FNAR_PASSWORD", "")
        self.FNAR_CSV_API_KEY = os.getenv("FNAR_CSV_API_KEY", "")
        
        # Polling configuration
        self.POLL_SECS = int(os.getenv("POLL_SECS", "300"))
        self.RETENTION_SEC = int(os.getenv("RETENTION_SEC", str(45*24*3600)))
    
    def _load_discord_config(self):
        """Load Discord bot configuration."""
        self.DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "")
        self.OWNER_ID = int(os.getenv("OWNER_ID", "0"))
        self.TARGET_GUILD_ID = int(os.getenv("TARGET_GUILD_ID", "0"))
        
        # Market source configuration
        self.MARKET_SOURCE = os.getenv("MARKET_SOURCE", "db").lower()
        self.N8N_WEBHOOK_URL = os.getenv("N8N_WEBHOOK_URL", "")
        self.N8N_MARKET_REPORT_URL = os.getenv("N8N_MARKET_REPORT_URL", "")
        
        # HTTP server configuration
        self.INBOUND_HOST = os.getenv("INBOUND_HOST", "0.0.0.0")
        self.INBOUND_PORT = int(os.getenv("INBOUND_PORT", "8081"))
        
        # Channel and role IDs
        self.REPORT_CHANNEL_ID = int(os.getenv("REPORT_CHANNEL_ID", "0"))
        self.ALERT_CHANNEL_ID = int(os.getenv("ALERT_CHANNEL_ID", "0"))
        self.ERROR_LOG_CHANNEL_ID = int(os.getenv("ERROR_LOG_CHANNEL_ID", "0"))
        self.MARKET_ALERT_ROLE_ID = int(os.getenv("MARKET_ALERT_ROLE_ID", "0"))
        
        # Excluded channels/categories
        self.EXCLUDED_CATEGORY_IDS = self._parse_id_list(os.getenv("EXCLUDED_CATEGORY_IDS", ""))
        self.EXCLUDED_CHANNEL_IDS = self._parse_id_list(os.getenv("EXCLUDED_CHANNEL_IDS", ""))
        
        # Bot naming
        self.PRIVATE_CATEGORY_NAME = os.getenv("PRIVATE_CATEGORY_NAME", "Private Bot Chats")
        self.PUBLIC_BOT_CHANNEL_NAME = os.getenv("PUBLIC_BOT_CHANNEL_NAME", "bot-commands")
        
        # Polling intervals
        self.MARKET_POLL_SECS = int(os.getenv("MARKET_POLL_SECS", "300"))
        self.USER_REPORT_SECS = int(os.getenv("USER_REPORT_SECS", "900"))
        
        # Market analysis
        self.ARBITRAGE_MIN_SPREAD_PCT = float(os.getenv("ARBITRAGE_MIN_SPREAD_PCT", "2.0"))
        self.WATCH_EXCHANGES = self._parse_exchange_list(os.getenv("WATCH_EXCHANGES", "AI1,CI1,IC1,NC1"))
        self.WATCH_RULES_PATH = os.getenv("WATCH_RULES_PATH", "./watch_rules.json")
    
    def _load_mcp_config(self):
        """Load MCP server configuration."""
        self.STATE_DIR = os.path.abspath(os.getenv("STATE_DIR", "./state"))
        self.TICK = float(os.getenv("TICK", "0.1"))
        self.HIST_BUCKET_SECS = int(os.getenv("HIST_BUCKET_SECS", "300"))
        
        # Ensure state directory exists
        os.makedirs(self.STATE_DIR, exist_ok=True)
    
    def _load_system_config(self):
        """Load system-wide configuration."""
        self.LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
        self.MAX_DISCORD_MSG_LEN = 2000
        self.LOG_SNIPPET = 200
        
        # Create log directory
        self.LOG_DIR = os.path.abspath("./log")
        os.makedirs(self.LOG_DIR, exist_ok=True)
        
        # Individual log files
        self.BOT_LOG = os.path.join(self.LOG_DIR, "bot.log")
        self.DATA_LOG = os.path.join(self.LOG_DIR, "data.log")
        self.MCP_LOG = os.path.join(self.LOG_DIR, "mcp.log")
        self.DB_LOG = os.path.join(self.LOG_DIR, "db.log")
        
        # Exchange list for data.py
        self.EXCHANGES = ["AI1", "CI1", "CI2", "NC1", "NC2", "IC1"]
    
    def _parse_id_list(self, value: str) -> Set[int]:
        """Parse comma-separated ID list."""
        ids: Set[int] = set()
        for item in (value or "").split(","):
            item = item.strip()
            if not item:
                continue
            try:
                ids.add(int(item))
            except ValueError:
                pass
        return ids
    
    def _parse_exchange_list(self, value: str) -> List[str]:
        """Parse comma-separated exchange list."""
        return [x.strip() for x in (value or "").split(",") if x.strip()]
    
    def validate_discord_config(self) -> List[str]:
        """Validate Discord configuration and return list of errors."""
        errors = []
        
        if not self.DISCORD_TOKEN:
            errors.append("DISCORD_TOKEN is required")
        
        if not self.TARGET_GUILD_ID:
            errors.append("TARGET_GUILD_ID is required")
        
        if not self.OWNER_ID:
            errors.append("OWNER_ID is required")
        
        return errors
    
    def validate_api_config(self) -> List[str]:
        """Validate API configuration and return list of errors."""
        errors = []
        
        if not self.FNAR_USERNAME:
            errors.append("FNAR_USERNAME is required for private data access")
        
        if not self.FNAR_CSV_API_KEY:
            errors.append("FNAR_CSV_API_KEY is required for private data access")
        
        return errors
    
    def setup_logging(self, component_name: str) -> logging.Logger:
        """Setup standardized logging for a component."""
        # Determine log file based on component
        log_file_map = {
            "prun-bot": self.BOT_LOG,
            "data.py": self.DATA_LOG,
            "prun-mcp": self.MCP_LOG,
            "dbops": self.DB_LOG,
            "dump": self.DB_LOG
        }
        
        log_file = log_file_map.get(component_name, os.path.join(self.LOG_DIR, f"{component_name}.log"))
        
        logging.basicConfig(
            level=getattr(logging, self.LOG_LEVEL, logging.INFO),
            format="%(asctime)s %(levelname)s %(name)s: %(message)s",
            handlers=[
                logging.FileHandler(log_file, encoding="utf-8", mode="a"),
                logging.StreamHandler()
            ]
        )
        return logging.getLogger(component_name)

# Global configuration instance
config = PrUnConfig()
