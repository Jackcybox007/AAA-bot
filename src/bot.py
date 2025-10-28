# src/bot.py
"""
PrUn Discord Market Hub Bot

Reads pre-rendered reports from tmp/reports and posts them.
- Server report -> #market-report, keep last 3 bot messages in that channel.
- User reports -> each user's private channel, based on user preferences.

Also supports watchlists, request locks, and n8n bridge as before.
"""

# Standard library
import asyncio
import hashlib
import json
import logging
import os
import signal
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path
from zoneinfo import ZoneInfo

# Third-party
import aiohttp
import aiosqlite
import discord
from aiohttp import web
from discord import app_commands, Object as _Obj
from discord.ext import commands, tasks
from dotenv import load_dotenv

# Local
from config import config
from market import (
    update_server_report as market_update_server_report,
    update_user_report as market_update_user_report,
)

# ========= env / config =========
load_dotenv()

DISCORD_TOKEN = config.DISCORD_TOKEN
MARKET_SOURCE = config.MARKET_SOURCE  # "db" | "n8n"
N8N_WEBHOOK_URL = config.N8N_WEBHOOK_URL
N8N_MARKET_REPORT_URL = config.N8N_MARKET_REPORT_URL
PRUN_DB_PATH = config.DB_PATH
INBOUND_HOST = config.INBOUND_HOST
INBOUND_PORT = config.INBOUND_PORT
OWNER_ID = config.OWNER_ID
TARGET_GUILD_ID = config.TARGET_GUILD_ID
_G = _Obj(id=TARGET_GUILD_ID)

EXCLUDED_CATEGORY_IDS = config.EXCLUDED_CATEGORY_IDS
EXCLUDED_CHANNEL_IDS = config.EXCLUDED_CHANNEL_IDS
ERROR_LOG_CHANNEL_ID = config.ERROR_LOG_CHANNEL_ID
REPORT_CHANNEL_ID = config.REPORT_CHANNEL_ID  # #market-report
ALERT_CHANNEL_ID = config.ALERT_CHANNEL_ID
MARKET_ALERT_ROLE_ID = config.MARKET_ALERT_ROLE_ID
PRIVATE_CATEGORY_NAME = config.PRIVATE_CATEGORY_NAME
PUBLIC_BOT_CHANNEL_NAME = config.PUBLIC_BOT_CHANNEL_NAME
BOT_DB = config.BOT_DB
USER_DB = config.USER_DB
MARKET_POLL_SECS = config.MARKET_POLL_SECS
USER_REPORT_SECS = config.USER_REPORT_SECS
ARBITRAGE_MIN_SPREAD_PCT = config.ARBITRAGE_MIN_SPREAD_PCT
DEFAULT_EXCHANGES = config.WATCH_EXCHANGES
WATCH_RULES_PATH = config.WATCH_RULES_PATH
MAX_DISCORD_MSG_LEN = config.MAX_DISCORD_MSG_LEN
LOG_SNIPPET = config.LOG_SNIPPET

LOCK_TTL_SECONDS = 15 * 60  # 15 minutes

# Report paths
REPORT_BASE = Path("tmp") / "reports"
REPORT_SERVER_FILE = REPORT_BASE / "server" / "report.md"
REPORT_USERS_DIR = REPORT_BASE / "users"

# ========= logging =========
log = config.setup_logging("prun-bot")

dbops = logging.getLogger("dbops")
dbops.setLevel(logging.INFO)
_dbops_handler = logging.FileHandler(config.DB_LOG, encoding="utf-8", mode="a")
_dbops_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
dbops.addHandler(_dbops_handler)

class DiscordErrorHandler(logging.Handler):
    def __init__(self, bot_ref: "commands.Bot", channel_id: int):
        super().__init__(level=logging.ERROR)
        self.bot_ref = bot_ref
        self.channel_id = channel_id
    async def _apost(self, text: str):
        try:
            ch = self.bot_ref.get_channel(self.channel_id)
            if ch:
                await ch.send(f"[error] {text[:1900]}")
        except Exception:
            pass
    def emit(self, record: logging.LogRecord):
        try:
            asyncio.get_running_loop().create_task(self._apost(self.format(record)))
        except Exception:
            pass

# ========= discord client =========
intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True
intents.members = True

bot = commands.Bot(command_prefix="?", intents=intents)
http_session: Optional[aiohttp.ClientSession] = None

# DB connections
db: Optional[aiosqlite.Connection] = None          # bot.db
userdb: Optional[aiosqlite.Connection] = None      # user.db

# in-memory
_watch_rules: List[Dict[str, Any]] = []
_last_user_digest: Dict[Tuple[int, int], str] = {}           # legacy
_last_server_digest: Optional[str] = None                     # file digest
_last_user_file_digest: Dict[Tuple[int, int], str] = {}       # (guild_id, user_id) -> digest
_start_time = datetime.now(timezone.utc)

# ========= utils =========
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def now_ts() -> int:
    return int(datetime.now(timezone.utc).timestamp())

def is_owner(user: discord.abc.User) -> bool:
    return int(user.id) == OWNER_ID

def channel_category_id(ch: discord.abc.GuildChannel) -> Optional[int]:
    if isinstance(ch, discord.Thread):
        p = ch.parent
        return p.category_id if p else None
    return getattr(ch, "category_id", None)

def is_excluded(ch: discord.abc.GuildChannel) -> bool:
    return ch.id in EXCLUDED_CHANNEL_IDS or (channel_category_id(ch) in EXCLUDED_CATEGORY_IDS)

def _snippet(s: str, n: int = LOG_SNIPPET) -> str:
    return (s[:n] + "…") if s and len(s) > n else (s or "")

def chunk_text(text: str, limit: int = MAX_DISCORD_MSG_LEN) -> List[str]:
    if not text:
        return []
    if len(text) <= limit:
        return [text]
    parts: List[str] = []
    buf = ""
    for seg in text.splitlines(True):  # keep newlines
        if len(seg) > limit:
            if buf:
                parts.append(buf); buf = ""
            for i in range(0, len(seg), limit):
                parts.append(seg[i:i+limit])
        else:
            if len(buf) + len(seg) <= limit:
                buf += seg
            else:
                parts.append(buf); buf = seg
    if buf:
        parts.append(buf)
    return parts

async def safe_send(channel: discord.abc.Messageable, content: Optional[str] = None, *, embed: Optional[discord.Embed] = None):
    if content is None:
        await channel.send(embed=embed); return
    chunks = chunk_text(content, MAX_DISCORD_MSG_LEN)
    if not chunks:
        await channel.send(embed=embed); return
    for i, c in enumerate(chunks):
        if i == 0 and embed is not None:
            await channel.send(content=c, embed=embed); embed = None
        else:
            await channel.send(content=c)

def read_text_or_none(p: Path) -> Optional[str]:
    try:
        if p.exists():
            return p.read_text(encoding="utf-8").strip()
    except Exception as e:
        log.error(f"read_text {p}: {e}")
    return None

async def prune_channel_reports(ch: discord.TextChannel, keep: int = 3):
    try:
        mine: List[discord.Message] = []
        async for m in ch.history(limit=200, oldest_first=False):
            if m.author.id == bot.user.id and isinstance(m.content, str) and m.content.startswith("**Market Report**"):
                mine.append(m)
        to_del = mine[keep:]
        for m in to_del:
            try: await m.delete()
            except Exception: pass
    except Exception as e:
        log.error(f"prune_channel_reports error: {e}")

# ========= bot DB schema =========
SCHEMA_SQL = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS server_watchlist(
  guild_id INTEGER NOT NULL,
  ticker   TEXT    NOT NULL,
  exchange TEXT    NOT NULL DEFAULT '',
  PRIMARY KEY(guild_id, ticker, exchange)
);

CREATE TABLE IF NOT EXISTS user_watchlist(
  guild_id INTEGER NOT NULL,
  user_id  INTEGER NOT NULL,
  ticker   TEXT    NOT NULL,
  exchange TEXT    NOT NULL DEFAULT '',
  PRIMARY KEY(guild_id, user_id, ticker, exchange)
);

CREATE TABLE IF NOT EXISTS user_meta(
  guild_id INTEGER NOT NULL,
  user_id  INTEGER NOT NULL,
  private_channel_id INTEGER,
  total_public_msgs  INTEGER NOT NULL DEFAULT 0,
  total_private_msgs INTEGER NOT NULL DEFAULT 0,
  active_lock        INTEGER NOT NULL DEFAULT 0,
  lock_acquired_at   INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY(guild_id, user_id)
);

CREATE TABLE IF NOT EXISTS user_report_prefs(
  guild_id INTEGER NOT NULL,
  user_id  INTEGER NOT NULL,
  tz       TEXT    NOT NULL DEFAULT 'UTC',
  freq_secs INTEGER NOT NULL DEFAULT 7200,
  window_start_min INTEGER NOT NULL DEFAULT 0,
  window_end_min   INTEGER NOT NULL DEFAULT 1440,
  enabled  INTEGER NOT NULL DEFAULT 0,
  last_sent_ts INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY(guild_id, user_id)
);
"""

USER_SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS users(
  discord_id TEXT PRIMARY KEY,
  fio_api_key TEXT NOT NULL,
  username TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
"""

def _ex_norm(cx: Optional[str]) -> str:
    return (cx or "").upper()

def _ex_out(cx: Optional[str]) -> Optional[str]:
    return None if (cx or "") == "" else cx

async def db_init():
    assert db is not None
    await db.executescript(SCHEMA_SQL)
    await db.commit()
    await _ensure_user_meta_columns()

async def userdb_init():
    assert userdb is not None
    await userdb.executescript(USER_SCHEMA_SQL); await userdb.commit()

# ---- schema backfill ----
async def _ensure_user_meta_columns():
    """Backfill columns for older installs."""
    assert db is not None
    cols = []
    async with db.execute("PRAGMA table_info(user_meta)") as cur:
        async for _, name, *_ in cur:
            cols.append(name)
    sqls = []
    if "total_public_msgs" not in cols:
        sqls.append("ALTER TABLE user_meta ADD COLUMN total_public_msgs INTEGER NOT NULL DEFAULT 0")
    if "total_private_msgs" not in cols:
        sqls.append("ALTER TABLE user_meta ADD COLUMN total_private_msgs INTEGER NOT NULL DEFAULT 0")
    if "active_lock" not in cols:
        sqls.append("ALTER TABLE user_meta ADD COLUMN active_lock INTEGER NOT NULL DEFAULT 0")
    if "lock_acquired_at" not in cols:
        sqls.append("ALTER TABLE user_meta ADD COLUMN lock_acquired_at INTEGER NOT NULL DEFAULT 0")
    for s in sqls:
        await db.execute(s)
    if sqls:
        await db.commit()

# ---- DB helpers (log to dbops) ----
async def db_add_server_watch(guild_id: int, ticker: str, exchange: Optional[str]):
    assert db is not None
    t, e = ticker.upper(), _ex_norm(exchange)
    await db.execute("INSERT OR IGNORE INTO server_watchlist(guild_id,ticker,exchange) VALUES(?,?,?)", (guild_id, t, e))
    await db.commit()
    msg = f"server_watchlist ADD guild={guild_id} ticker={t} exchange={e or 'ALL'}"
    log.info(msg); dbops.info(msg)

async def db_del_server_watch(guild_id: int, ticker: str, exchange: Optional[str]):
    assert db is not None
    t = ticker.upper()
    if exchange:
        e = _ex_norm(exchange)
        await db.execute("DELETE FROM server_watchlist WHERE guild_id=? AND ticker=? AND exchange=?", (guild_id, t, e))
        msg = f"server_watchlist DEL guild={guild_id} ticker={t} exchange={e}"
    else:
        await db.execute("DELETE FROM server_watchlist WHERE guild_id=? AND ticker=?", (guild_id, t))
        msg = f"server_watchlist DEL-ALL guild={guild_id} ticker={t}"
    await db.commit()
    log.info(msg); dbops.info(msg)

async def db_get_server_watch(guild_id: int) -> List[Tuple[str, Optional[str]]]:
    assert db is not None
    cur = await db.execute("SELECT ticker, exchange FROM server_watchlist WHERE guild_id=? ORDER BY ticker, exchange", (guild_id,))
    rows = await cur.fetchall()
    res = [(r[0], _ex_out(r[1])) for r in rows]
    dbops.info(f"server_watchlist LIST guild={guild_id} count={len(res)}")
    return res

async def db_add_user_watch(gid: int, uid: int, ticker: str, exchange: Optional[str]):
    assert db is not None
    t, e = ticker.upper(), _ex_norm(exchange)
    await db.execute("INSERT OR IGNORE INTO user_watchlist(guild_id,user_id,ticker,exchange) VALUES(?,?,?,?)", (gid, uid, t, e))
    await db.commit()
    msg = f"user_watchlist ADD guild={gid} user={uid} ticker={t} exchange={e or 'ALL'}"
    log.info(msg); dbops.info(msg)

async def db_del_user_watch(gid: int, uid: int, ticker: str, exchange: Optional[str]):
    assert db is not None
    t = ticker.upper()
    if exchange:
        e = _ex_norm(exchange)
        await db.execute("DELETE FROM user_watchlist WHERE guild_id=? AND user_id=? AND ticker=? AND exchange=?", (gid, uid, t, e))
        msg = f"user_watchlist DEL guild={gid} user={uid} ticker={t} exchange={e}"
    else:
        await db.execute("DELETE FROM user_watchlist WHERE guild_id=? AND user_id=? AND ticker=?", (gid, uid, t))
        msg = f"user_watchlist DEL-ALL guild={gid} user={uid} ticker={t}"
    await db.commit()
    log.info(msg); dbops.info(msg)

async def db_get_user_watch(gid: int, uid: int) -> List[Tuple[str, Optional[str]]]:
    assert db is not None
    cur = await db.execute("SELECT ticker, exchange FROM user_watchlist WHERE guild_id=? AND user_id=? ORDER BY ticker, exchange", (gid, uid))
    rows = await cur.fetchall()
    res = [(r[0], _ex_out(r[1])) for r in rows]
    dbops.info(f"user_watchlist LIST guild={gid} user={uid} count={len(res)}")
    return res

async def db_cache_private_channel(gid: int, uid: int, ch_id: int):
    assert db is not None
    await db.execute(
        "INSERT INTO user_meta(guild_id,user_id,private_channel_id) VALUES(?,?,?) "
        "ON CONFLICT(guild_id,user_id) DO UPDATE SET private_channel_id=excluded.private_channel_id",
        (gid, uid, ch_id)
    ); await db.commit()
    msg = f"user_meta UPSERT guild={gid} user={uid} private_channel_id={ch_id}"
    log.info(msg); dbops.info(msg)

async def db_get_private_channel(gid: int, uid: int) -> Optional[int]:
    assert db is not None
    cur = await db.execute("SELECT private_channel_id FROM user_meta WHERE guild_id=? AND user_id=?", (gid, uid))
    row = await cur.fetchone()
    val = int(row[0]) if row and row[0] else None
    dbops.info(f"user_meta GET guild={gid} user={uid} private_channel_id={val}")
    return val

# ========= user.db helpers =========
async def user_upsert(discord_id: int, fio_api_key: Optional[str], username: Optional[str]):
    """Upsert FNAR key and optional in-game username."""
    assert userdb is not None
    ts = now_ts()
    await userdb.execute(
        "INSERT INTO users(discord_id,fio_api_key,username,created_at,updated_at) VALUES(?,?,?,?,?) "
        "ON CONFLICT(discord_id) DO UPDATE SET "
        "  fio_api_key=COALESCE(excluded.fio_api_key, users.fio_api_key), "
        "  username=COALESCE(excluded.username, users.username), "
        "  updated_at=excluded.updated_at",
        (str(discord_id), fio_api_key, username, ts, ts)
    )
    await userdb.commit()
    dbops.info(f"user UPSERT id={discord_id} key_set={bool(fio_api_key)} username_set={bool(username)}")

async def user_get_key(discord_id: int) -> Optional[str]:
    assert userdb is not None
    cur = await userdb.execute("SELECT fio_api_key FROM users WHERE discord_id=?", (str(discord_id),))
    row = await cur.fetchone()
    val = row[0] if row else None
    dbops.info(f"user_key GET discord_id={discord_id} exists={bool(val)}")
    return val

# ========= request-locking =========
async def _begin_immediate(conn: aiosqlite.Connection):
    await conn.execute("BEGIN IMMEDIATE")

async def is_user_private_channel(guild: discord.Guild, channel: discord.abc.GuildChannel, user_id: int) -> bool:
    ch_id = await db_get_private_channel(guild.id, user_id)
    if ch_id and int(ch_id) == int(channel.id):
        return True
    cat = getattr(channel, "category", None)
    if cat and cat.name == PRIVATE_CATEGORY_NAME:
        topic = getattr(channel, "topic", "") or ""
        if f"private-bot:{user_id}" in topic:
            return True
    return False

async def acquire_user_lock(guild_id: int, user_id: int) -> bool:
    """Atomic 1-slot lock per user across the guild using user_meta.active_lock."""
    assert db is not None
    try:
        await _begin_immediate(db)
        await db.execute(
            "INSERT INTO user_meta(guild_id,user_id,active_lock,lock_acquired_at) VALUES(?,?,0,0) "
            "ON CONFLICT(guild_id,user_id) DO NOTHING",
            (guild_id, user_id),
        )
        cur = await db.execute("SELECT active_lock FROM user_meta WHERE guild_id=? AND user_id=?", (guild_id, user_id))
        row = await cur.fetchone()
        val = int(row[0] or 0) if row else 0
        if val >= 1:
            await db.execute("ROLLBACK")
            return False
        await db.execute(
            "UPDATE user_meta SET active_lock=1, lock_acquired_at=? WHERE guild_id=? AND user_id=?",
            (now_ts(), guild_id, user_id),
        )
        await db.commit()
        return True
    except Exception as e:
        log.error(f"acquire_user_lock error: {e}")
        try: await db.execute("ROLLBACK")
        except Exception: pass
        return False

async def release_user_lock(guild_id: int, user_id: int) -> None:
    assert db is not None
    try:
        await _begin_immediate(db)
        await db.execute("UPDATE user_meta SET active_lock=0, lock_acquired_at=0 WHERE guild_id=? AND user_id=?", (guild_id, user_id))
        await db.commit()
    except Exception as e:
        log.error(f"release_user_lock error: {e}")
        try: await db.execute("ROLLBACK")
        except Exception: pass

# ========= stale lock reaper =========
@tasks.loop(seconds=60)
async def stale_lock_reaper():
    try:
        assert db is not None
        cutoff = now_ts() - LOCK_TTL_SECONDS
        await db.execute(
            "UPDATE user_meta SET active_lock=0, lock_acquired_at=0 WHERE active_lock=1 AND lock_acquired_at>0 AND lock_acquired_at<?",
            (cutoff,),
        )
        await db.commit()
    except Exception as e:
        log.error(f"stale_lock_reaper error: {e}")

@stale_lock_reaper.before_loop
async def before_reaper():
    await bot.wait_until_ready()

# ========= slash sync cleanup =========
async def sync_slash_clean():
    bot.tree.clear_commands(guild=None)
    await bot.tree.sync(guild=None)
    synced = await bot.tree.sync(guild=_G)
    log.info(f"slash synced guild={TARGET_GUILD_ID} count={len(synced)}")

# ========= watch rules (legacy) =========
async def load_watch_rules():
    global _watch_rules
    try:
        if os.path.exists(WATCH_RULES_PATH):
            with open(WATCH_RULES_PATH, "r", encoding="utf-8") as f:
                _watch_rules = json.load(f)
                if not isinstance(_watch_rules, list):
                    _watch_rules = []
        else:
            _watch_rules = []
    except Exception as e:
        log.error(f"failed to load watch rules: {e}")
        _watch_rules = []

# ========= user-report prefs =========
async def prefs_get(gid: int, uid: int) -> Dict[str, Any]:
    assert db is not None
    row = await (await db.execute(
        "SELECT tz,freq_secs,window_start_min,window_end_min,enabled,last_sent_ts FROM user_report_prefs WHERE guild_id=? AND user_id=?",
        (gid, uid)
    )).fetchone()
    if not row:
        return {"tz":"UTC","freq_secs":USER_REPORT_SECS,"window_start_min":0,"window_end_min":1440,"enabled":1,"last_sent_ts":0}
    tz, f, s, e, en, ls = row
    return {"tz":tz,"freq_secs":int(f),"window_start_min":int(s),"window_end_min":int(e),"enabled":int(en),"last_sent_ts":int(ls)}

async def prefs_set(gid: int, uid: int, tz: str, freq_secs: int, start_min: int, end_min: int, enabled: int):
    assert db is not None
    await db.execute(
        "INSERT INTO user_report_prefs(guild_id,user_id,tz,freq_secs,window_start_min,window_end_min,enabled,last_sent_ts) "
        "VALUES(?,?,?,?,?,?,?,0) "
        "ON CONFLICT(guild_id,user_id) DO UPDATE SET tz=excluded.tz,freq_secs=excluded.freq_secs, "
        "window_start_min=excluded.window_start_min, window_end_min=excluded.window_end_min, enabled=excluded.enabled",
        (gid, uid, tz, int(freq_secs), int(start_min), int(end_min), int(enabled))
    ); await db.commit()

async def prefs_touch_sent(gid: int, uid: int, ts: int):
    assert db is not None
    await db.execute("UPDATE user_report_prefs SET last_sent_ts=? WHERE guild_id=? AND user_id=?", (ts, gid, uid))
    await db.commit()

def _parse_hhmm(s: str) -> Optional[int]:
    try:
        h, m = s.strip().split(":"); h = int(h); m = int(m)
        if 0 <= h < 24 and 0 <= m < 60:
            return h*60 + m
    except Exception:
        return None
    return None

def _in_local_window(now_ts: int, tzname: str, start_min: int, end_min: int) -> bool:
    try:
        tz = ZoneInfo(tzname)
    except Exception:
        tz = timezone.utc
    dt = datetime.fromtimestamp(now_ts, tz=tz)
    cur = dt.hour*60 + dt.minute
    if start_min <= end_min:
        return start_min <= cur < end_min
    # overnight window (e.g., 22:00–06:00)
    return cur >= start_min or cur < end_min

# ========= market I/O (legacy support for n8n/db snapshot commands) =========
async def n8n_snapshot(tickers: List[str], exchanges: List[str]) -> Dict[str, Dict[str, Dict[str, Any]]]:
    if not N8N_MARKET_REPORT_URL:
        return {}
    assert http_session is not None
    payload = {"action": "snapshot", "tickers": tickers, "exchanges": exchanges}
    try:
        async with http_session.post(N8N_MARKET_REPORT_URL, json=payload, timeout=180) as resp:
            body = await resp.text()
            log.info(f"n8n_snapshot status={resp.status} bytes={len(body)} tickers={len(tickers)} exchanges={len(exchanges)}")
            if resp.status != 200:
                log.error(f"n8n snapshot HTTP {resp.status}: {_snippet(body)}")
                return {}
            try:
                return await resp.json()
            except Exception as je:
                log.error(f"n8n snapshot json error: {je}")
                return {}
    except Exception as e:
        log.error(f"n8n snapshot error: {e}")
        return {}

async def db_snapshot(items: List[Tuple[str, Optional[str]]]) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """Return {ticker: {cx: {bid,ask,ts}}} using prices only."""
    if not items:
        return {}
    tickers = sorted({t.upper() for t, _ in items})
    explicit_ex = sorted({(ex or "").upper() for _, ex in items if ex})
    exchanges = (explicit_ex if explicit_ex else DEFAULT_EXCHANGES)
    log.info(f"db_snapshot tickers={len(tickers)} exchanges={exchanges}")

    uri = f"file:{PRUN_DB_PATH}?mode=ro"
    async with aiosqlite.connect(uri, uri=True) as con:
        con.row_factory = aiosqlite.Row
        out: Dict[str, Dict[str, Dict[str, Any]]] = {t: {} for t in tickers}
        for t in tickers:
            for cx in exchanges:
                cur = await con.execute("SELECT best_bid, best_ask, ts FROM prices WHERE cx=? AND ticker=?", (cx, t))
                row = await cur.fetchone()
                if not row:
                    continue
                bb, ba, ts_val = row["best_bid"], row["best_ask"], row["ts"]
                out[t][cx] = {"bid": bb, "ask": ba, "ts": ts_val}
        return out

async def market_snapshot(items: List[Tuple[str, Optional[str]]]) -> Dict[str, Dict[str, Dict[str, Any]]]:
    if MARKET_SOURCE == "n8n":
        tickers = sorted({t.upper() for t, _ in items})
        explicit_ex = sorted({(ex or "").upper() for _, ex in items if ex})
        exchanges = (explicit_ex if explicit_ex else DEFAULT_EXCHANGES)
        return await n8n_snapshot(tickers, exchanges) or {}
    return await db_snapshot(items)

def best_arbitrage_for_ticker(rows: Dict[str, Dict[str, Any]]) -> Optional[Tuple[str, str, float, float, float]]:
    best_buy = None; best_sell = None
    for cx, v in rows.items():
        ask = v.get("ask"); bid = v.get("bid")
        if isinstance(ask, (int, float)) and ask > 0:
            if best_buy is None or ask < best_buy[1]:
                best_buy = (cx, float(ask))
        if isinstance(bid, (int, float)) and bid > 0:
            if best_sell is None or bid > best_sell[1]:
                best_sell = (cx, float(bid))
    if not best_buy or not best_sell:
        return None
    bc, bp = best_buy; sc, sp = best_sell
    if sp <= bp:
        return None
    pct = (sp - bp) / bp * 100.0
    return bc, sc, bp, sp, pct

# ========= file-based report loops =========
@tasks.loop(seconds=MARKET_POLL_SECS)
async def market_report_loop():
    """Server report: update only the server report and post if changed."""
    if not REPORT_CHANNEL_ID:
        return
    ch = bot.get_channel(REPORT_CHANNEL_ID)
    if ch is None or not isinstance(ch, discord.TextChannel):
        return
    try:
        log.info("market_report_loop: updating server report only")
        # Offload blocking work
        await asyncio.to_thread(market_update_server_report)
        txt = await asyncio.to_thread(read_text_or_none, REPORT_SERVER_FILE)
        if not txt:
            return
        dg = hashlib.sha256(txt.encode("utf-8")).hexdigest()
        global _last_server_digest
        if _last_server_digest == dg:
            log.info("market_report_loop: no change")
            return
        _last_server_digest = dg
        await safe_send(ch, txt)
        log.info("market_report_loop: sent")
        await prune_channel_reports(ch, keep=3)
    except Exception as e:
        log.error(f"market_report_loop error: {e}")

@market_report_loop.before_loop
async def before_market_report():
    await bot.wait_until_ready()

@tasks.loop(seconds=USER_REPORT_SECS)
async def user_private_loop():
    """User reports: generate and send per-user only when due."""
    try:
        for g in bot.guilds:
            if g.id != TARGET_GUILD_ID:
                continue
            for m in g.members:
                log.info(f"user_private_loop: checking member {m.name} {m.id}")
                if m.bot:
                    continue
                prefs = await prefs_get(g.id, m.id)
                if not prefs["enabled"]:
                    continue
                now = now_ts()
                # frequency gate
                if now - int(prefs["last_sent_ts"]) < int(prefs["freq_secs"]) - 10:
                    continue
                # time window gate
                if not _in_local_window(now, prefs["tz"], prefs["window_start_min"], prefs["window_end_min"]):
                    continue

                # ensure private channel
                ch_id = await db_get_private_channel(g.id, m.id)
                ch = g.get_channel(ch_id) if ch_id else None
                if ch is None:
                    ch = await ensure_member_private_channel(g, m)
                    if ch:
                        await db_cache_private_channel(g.id, m.id, ch.id)
                if ch is None:
                    continue

                # UPDATE only this user's report off-thread
                try:
                    await asyncio.to_thread(market_update_user_report, str(m.id))
                except Exception as e:
                    log.error(f"user_private_loop update_user_report error uid={m.id}: {e}")
                    continue

                p = REPORT_USERS_DIR / str(m.id) / "report.md"
                txt = await asyncio.to_thread(read_text_or_none, p)
                if not txt:
                    continue

                key = (g.id, m.id)
                dg = hashlib.sha256(txt.encode("utf-8")).hexdigest()
                if _last_user_file_digest.get(key) == dg:
                    continue

                try:
                    await safe_send(ch, txt)
                    _last_user_file_digest[key] = dg
                    await prefs_touch_sent(g.id, m.id, now)
                    log.info(f"user_private_loop: sent to {m.name} {m.id}")
                except Exception as e:
                    log.error(f"user_private_loop send error uid={m.id}: {e}")
    except Exception as e:
        log.error(f"user_private_loop error: {e}")

@user_private_loop.before_loop
async def before_user_private():
    await bot.wait_until_ready()

# ========= HTTP inbound bridge (also releases user locks) =========
async def handle_health(request: web.Request) -> web.StreamResponse:
    return web.json_response({"ok": True, "service": "prun-discord-bridge", "time": now_iso(), "source": MARKET_SOURCE})

async def handle_incoming(request: web.Request) -> web.StreamResponse:
    """Inbound from n8n. Echo to Discord. Release user lock when discordID matches."""
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid_json"}, status=400)

    for k in ("message", "channelID", "serverID"):
        if k not in data:
            return web.json_response({"ok": False, "error": f"missing_{k}"}, status=400)

    msg_text = str(data["message"])
    try:
        channel_id = int(data["channelID"])
    except ValueError:
        return web.json_response({"ok": False, "error": "channelID_not_int"}, status=400)
    server_id = str(data.get("serverID", "0"))
    discord_id = str(data.get("discordID", ""))  # needed to release
    log.info(f"inbound msg len={len(msg_text)} ch={channel_id} srv={server_id} uid={discord_id or '-'} snip='{_snippet(msg_text)}'")

    channel = bot.get_channel(channel_id)
    if channel is None:
        try:
            gid = int(server_id)
            if gid > 0:
                guild = bot.get_guild(gid)
                if guild:
                    channel = await guild.fetch_channel(channel_id)
        except Exception as e:
            log.error(f"inbound fetch_channel error: {e}")
            channel = None
    if channel is None:
        log.error(f"inbound: channel not found channelID={channel_id} serverID={server_id}")
        try:
            gid = int(server_id)
            uid = int(discord_id) if discord_id.isdigit() else None
            if gid > 0 and uid is not None:
                await release_user_lock(gid, uid)
        except Exception:
            pass
        return web.json_response({"ok": False, "error": "channel_not_found"}, status=404)

    try:
        if len(msg_text) > MAX_DISCORD_MSG_LEN:
            chunks = chunk_text(msg_text, MAX_DISCORD_MSG_LEN)
            for c in chunks:
                await channel.send(c)
        else:
            await channel.send(msg_text)
    except Exception as e:
        log.error(f"inbound send failed: {e}")

    try:
        gid = int(server_id)
        uid = int(discord_id) if discord_id.isdigit() else None
        if gid > 0 and uid is not None:
            await release_user_lock(gid, uid)
            log.info(f"released user lock gid={gid} uid={uid}")
    except Exception as e:
        log.error(f"inbound release lock error: {e}")

    return web.json_response({"ok": True})

async def start_http_server() -> web.AppRunner:
    app = web.Application()
    app.add_routes([web.get("/health", handle_health), web.post("/", handle_incoming)])
    runner = web.AppRunner(app); await runner.setup()
    site = web.TCPSite(runner, INBOUND_HOST, INBOUND_PORT); await site.start()
    log.info(f"inbound HTTP listening on http://{INBOUND_PORT}/")
    return runner

# ========= private channel helpers =========
async def find_or_create_private_category(guild: discord.Guild) -> discord.CategoryChannel:
    for c in guild.categories:
        if c.name == PRIVATE_CATEGORY_NAME:
            return c
    overwrites = {
        guild.default_role: discord.PermissionOverwrite(view_channel=False),
        guild.me: discord.PermissionOverwrite(view_channel=True, manage_channels=True, send_messages=True, read_message_history=True),
    }
    cat = await guild.create_category(PRIVATE_CATEGORY_NAME, overwrites=overwrites, reason="Private bot chats")
    log.info(f"created private category guild={guild.id} cat={cat.id}")
    return cat

async def ensure_member_private_channel(guild: discord.Guild, member: discord.Member) -> Optional[discord.TextChannel]:
    if member.bot:
        return None
    cat = await find_or_create_private_category(guild)
    tag = f"private-bot:{member.id}"
    for ch in cat.text_channels:
        if ch.topic and tag in ch.topic:
            return ch
    overwrites = {
        guild.default_role: discord.PermissionOverwrite(view_channel=False),
        member: discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True),
        guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True, manage_channels=True, read_message_history=True),
    }
    name = f"bot-{member.name[:20].lower()}-{str(member.id)[-4:]}"
    ch = await guild.create_text_channel(
        name, category=cat, overwrites=overwrites,
        topic=f"{tag} | Private chat with the bot", reason="Per-member private bot channel"
    )
    log.info(f"created private channel guild={guild.id} user={member.id} channel={ch.id}")
    return ch

# ========= events =========
@bot.event
async def on_ready():
    log.info(f"ready as {bot.user} id={bot.user.id} guilds={len(bot.guilds)} members={sum(len(g.members) for g in bot.guilds)}")
    if ERROR_LOG_CHANNEL_ID:
        h = DiscordErrorHandler(bot, ERROR_LOG_CHANNEL_ID)
        h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
        logging.getLogger().addHandler(h)

    for g in bot.guilds:
        if g.id != TARGET_GUILD_ID:
            continue
        for m in g.members:
            if m.bot:
                continue
            try:
                ch = await ensure_member_private_channel(g, m)
                if ch and db:
                    await db_cache_private_channel(g.id, m.id, ch.id)
            except discord.Forbidden:
                log.error(f"missing perms for member={m.id} guild={g.id}")
            except Exception as e:
                log.error(f"ensure channel error member={m.id} guild={g.id}: {e}")

    if REPORT_CHANNEL_ID and not market_report_loop.is_running():
        market_report_loop.start()
    if not user_private_loop.is_running():
        user_private_loop.start()
    if not stale_lock_reaper.is_running():
        stale_lock_reaper.start()

    try:
        await sync_slash_clean()
    except Exception as e:
        log.error(f"slash sync failed: {e}")

# Suppress CommandNotFound for prefixed messages
@bot.event
async def on_command_error(ctx: commands.Context, error: commands.CommandError):
    if isinstance(error, commands.CommandNotFound):
        return
    log.error(f"command error: {error}")

@bot.event
async def on_member_join(member: discord.Member):
    if member.guild.id != TARGET_GUILD_ID:
        return
    try:
        ch = await ensure_member_private_channel(member.guild, member)
        if ch and db:
            await db_cache_private_channel(member.guild.id, member.id, ch.id)
        tgt = member.guild.system_channel or next((c for c in member.guild.text_channels if c.permissions_for(member.guild.me).send_messages), None)
        if tgt:
            await tgt.send(f"Welcome {member.mention}. A private channel has been created for you to chat with the bot.")
        if ch:
            await ch.send(f"Hi {member.mention}. This is your private channel with the bot. Please read https://discord.com/channels/1418530427471269921/1430844045621723236/1430859783732461608 for more information")
        log.info(f"member_join uid={member.id} guild={member.guild.id} private_ch={(ch.id if ch else None)}")
    except Exception as e:
        log.error(f"on_member_join error: {e}")

# ========= message handler: single DB-backed lock, '?' only =========
@bot.event
async def on_message(message: discord.Message):
    if message.author == bot.user or getattr(message.author, "bot", False):
        return
    if not message.guild or message.guild.id != TARGET_GUILD_ID:
        return

    content = message.content or ""
    if not content.startswith("?"):
        return
    if is_excluded(message.channel):
        return

    user_id = int(message.author.id)
    guild_id = int(message.guild.id)

    got_lock = await acquire_user_lock(guild_id, user_id)
    if not got_lock:
        await safe_send(message.channel, "You already have an active request. Wait for the previous response or ask an admin to release it.")
        return

    await safe_send(message.channel, "Processing your request, Please wait")

    payload = {
        "message": content.replace("?", "", 1),
        "channelID": str(message.channel.id),
        "serverID": str(guild_id),
        "discordID": str(user_id),
    }
    try:
        assert http_session is not None
        async with http_session.post(N8N_WEBHOOK_URL, json=payload, timeout=180) as resp:
            txt = await resp.text()
            log.info(f"n8n tx status={resp.status} bytes={len(txt)} uid={user_id}")
            if resp.status != 200:
                log.error(f"n8n HTTP {resp.status}: {_snippet(txt)}")
    except Exception as e:
        log.error(f"webhook exception: {e}")

# ========= slash commands =========
@bot.tree.command(name="ping", description="Bot latency and uptime")
@app_commands.guilds(_G)
async def ping_cmd(interaction: discord.Interaction):
    delta = datetime.now(timezone.utc) - _start_time
    await interaction.response.send_message(f"pong {bot.latency*1000:.0f} ms | up {delta}", ephemeral=True)

@bot.tree.command(name="whoami", description="Show your Discord ID")
@app_commands.guilds(_G)
async def whoami_cmd(interaction: discord.Interaction):
    await interaction.response.send_message(f"user {interaction.user} id {interaction.user.id}", ephemeral=True)

@bot.tree.command(name="diag_perms", description="Show my key permissions here")
@app_commands.guilds(_G)
async def diag_perms_cmd(i: discord.Interaction):
    me = i.guild.me
    p = i.channel.permissions_for(me)
    txt = (
        f"Manage Channels: {p.manage_channels}\n"
        f"Manage Roles: {p.manage_roles}\n"
        f"View Channel: {p.view_channel}\n"
        f"Send Messages: {p.send_messages}\n"
        f"Read Message History: {p.read_message_history}"
    )
    await i.response.send_message(txt, ephemeral=True)

@bot.tree.command(name="info", description="Bot info")
@app_commands.guilds(_G)
async def info_cmd(interaction: discord.Interaction):
    await interaction.response.send_message(
        f"AXO Market Sentinel | source={MARKET_SOURCE} | poll={MARKET_POLL_SECS}s | private={USER_REPORT_SECS}s | db={BOT_DB}",
        ephemeral=True
    )

@bot.tree.command(name="help", description="Commands help")
@app_commands.guilds(_G)
async def help_cmd(interaction: discord.Interaction):
    txt = (
        "/register (fio_api_key) [username]\n"
        "/keyshow\n"
        "/watchlistadd (TICKER) [CX]\n"
        "/watchlistremove (TICKER) [CX]\n"
        "/watchlist\n"
        "/privateadd (TICKER) [CX]\n"
        "/privateremove (TICKER) [CX]\n"
        "/privatelist\n"
        "/watchlistclear  |  /privateclear\n"
        "/report_prefs_set tz every_minutes window_start window_end\n"
        "/report_prefs_show | /report_prefs_enable | /report_prefs_disable\n"
        "/report_now\n"
        "/subscribe | /unsubscribe\n"
        "/ping | /whoami | /diag_perms"
    )
    await interaction.response.send_message(txt, ephemeral=True)

# --- Registration ---
@bot.tree.command(name="register", description="Register your FNAR CSV key and optional in-game username")
@app_commands.guilds(_G)
@app_commands.describe(
    fio_api_key="Your FIO API key",
    username="Your FIO username"
)
async def register_cmd(interaction: discord.Interaction, fio_api_key: str, username: str):
    await user_upsert(interaction.user.id, fio_api_key, username)
    try:
        cat = await find_or_create_private_category(interaction.guild)
        in_private = (channel_category_id(interaction.channel) == cat.id)
        if not in_private:
            ch = await ensure_member_private_channel(interaction.guild, interaction.user)
            if ch and db:
                await db_cache_private_channel(interaction.guild.id, interaction.user.id, ch.id)
    except Exception as e:
        log.error(f"register ensure private failed: {e}")
    msg = "Key saved." + (f" Username set to '{username}'." if username else "")
    await interaction.response.send_message(msg, ephemeral=True)

@bot.tree.command(name="keyshow", description="Show your stored key (masked)")
@app_commands.guilds(_G)
async def keyshow_cmd(interaction: discord.Interaction):
    k = await user_get_key(interaction.user.id)
    if not k:
        await interaction.response.send_message("No key on file.", ephemeral=True); return
    masked = (k[:4] + "*"*max(0, len(k)-8) + k[-4:]) if len(k) >= 8 else k
    await interaction.response.send_message(masked, ephemeral=True)

# --- Watchlists: server ---
@bot.tree.command(name="watchlistadd", description="Add to server-wide watch list")
@app_commands.guilds(_G)
@app_commands.describe(ticker="e.g. RAT", cx="Optional exchange, e.g. CI1")
async def watchlistadd_cmd(interaction: discord.Interaction, ticker: str, cx: Optional[str] = None):
    if not interaction.user.guild_permissions.manage_guild and not interaction.user.guild_permissions.manage_messages:
        await interaction.response.send_message("Need Manage Messages or Manage Server.", ephemeral=True); return
    await db_add_server_watch(interaction.guild.id, ticker, cx)
    await interaction.response.send_message("Added.", ephemeral=True)

@bot.tree.command(name="watchlistremove", description="Remove from server-wide watch list")
@app_commands.guilds(_G)
@app_commands.describe(ticker="e.g. RAT", cx="Optional exchange, e.g. CI1")
async def watchlistremove_cmd(interaction: discord.Interaction, ticker: str, cx: Optional[str] = None):
    if not interaction.user.guild_permissions.manage_guild and not interaction.user.guild_permissions.manage_messages:
        await interaction.response.send_message("Need Manage Messages or Manage Server.", ephemeral=True); return
    await db_del_server_watch(interaction.guild.id, ticker, cx)
    await interaction.response.send_message("Removed.", ephemeral=True)

@bot.tree.command(name="watchlist", description="Show server-wide watch list")
@app_commands.guilds(_G)
async def watchlist_cmd(interaction: discord.Interaction):
    items = await db_get_server_watch(interaction.guild.id)
    if not items:
        await interaction.response.send_message("Server watchlist is empty.", ephemeral=True); return
    from collections import defaultdict
    g = defaultdict(list)
    for t, ex in items:
        g[t.upper()].append((ex or "ALL"))
    lines = [f"{t}: " + ", ".join(sorted(set(exs))) for t, exs in sorted(g.items())]
    await interaction.response.send_message("**Server Watchlist**\n" + "\n".join(lines), ephemeral=True)

@bot.tree.command(name="watchlistclear", description="Clear the entire server-wide watch list")
@app_commands.guilds(_G)
async def watchlistclear_cmd(interaction: discord.Interaction):
    if not interaction.user.guild_permissions.manage_guild:
        await interaction.response.send_message("Need Manage Server.", ephemeral=True); return
    assert db is not None
    await db.execute("DELETE FROM server_watchlist WHERE guild_id=?", (interaction.guild.id,))
    await db.commit()
    msg = f"server_watchlist CLEAR guild={interaction.guild.id}"
    log.info(msg); dbops.info(msg)
    await interaction.response.send_message("Server watchlist cleared.", ephemeral=True)

# --- Watchlists: private ---
@bot.tree.command(name="privateadd", description="Add to your private watch list")
@app_commands.guilds(_G)
@app_commands.describe(ticker="e.g. RAT", cx="Optional exchange, e.g. CI1")
async def privateadd_cmd(interaction: discord.Interaction, ticker: str, cx: Optional[str] = None):
    await db_add_user_watch(interaction.guild.id, interaction.user.id, ticker, cx)
    await interaction.response.send_message("Added to your list.", ephemeral=True)

@bot.tree.command(name="privateremove", description="Remove from your private watch list")
@app_commands.guilds(_G)
@app_commands.describe(ticker="e.g. RAT", cx="Optional exchange, e.g. CI1")
async def privateremove_cmd(interaction: discord.Interaction, ticker: str, cx: Optional[str] = None):
    await db_del_user_watch(interaction.guild.id, interaction.user.id, ticker, cx)
    await interaction.response.send_message("Removed from your list.", ephemeral=True)

@bot.tree.command(name="privatelist", description="Show your private watch list")
@app_commands.guilds(_G)
async def privatelist_cmd(interaction: discord.Interaction):
    items = await db_get_user_watch(interaction.guild.id, interaction.user.id)
    if not items:
        await interaction.response.send_message("Your private watchlist is empty.", ephemeral=True); return
    from collections import defaultdict
    g = defaultdict(list)
    for t, ex in items:
        g[t.upper()].append((ex or "ALL"))
    lines = [f"{t}: " + ", ".join(sorted(set(exs))) for t, exs in sorted(g.items())]
    await interaction.response.send_message("**Your Private Watchlist**\n" + "\n".join(lines), ephemeral=True)

@bot.tree.command(name="privateclear", description="Clear your private watch list")
@app_commands.guilds(_G)
async def privateclear_cmd(interaction: discord.Interaction):
    assert db is not None
    await db.execute("DELETE FROM user_watchlist WHERE guild_id=? AND user_id=?", (interaction.guild.id, interaction.user.id))
    await db.commit()
    msg = f"user_watchlist CLEAR guild={interaction.guild.id} user={interaction.user.id}"
    log.info(msg); dbops.info(msg)
    await interaction.response.send_message("Your private watchlist cleared.", ephemeral=True)

# --- Report prefs commands ---
@bot.tree.command(name="report_prefs_set", description="Set your private report schedule")
@app_commands.guilds(_G)
@app_commands.describe(
    tz="IANA timezone, e.g., Europe/London",
    every_minutes="Frequency in minutes (>=5)",
    window_start="HH:MM local start (inclusive)",
    window_end="HH:MM local end (exclusive)"
)
async def report_prefs_set_cmd(i: discord.Interaction, tz: str, every_minutes: int, window_start: str, window_end: str):
    sm = _parse_hhmm(window_start); em = _parse_hhmm(window_end)
    if sm is None or em is None or every_minutes < 5:
        await i.response.send_message("Invalid time or frequency. Min frequency 5 minutes. Time format HH:MM.", ephemeral=True); return
    try: ZoneInfo(tz)
    except Exception:
        await i.response.send_message("Invalid timezone. Use an IANA tz like Europe/London.", ephemeral=True); return
    await prefs_set(i.guild.id, i.user.id, tz, every_minutes*60, sm, em, 1)
    await i.response.send_message("Saved.", ephemeral=True)

@bot.tree.command(name="report_prefs_show", description="Show your private report schedule")
@app_commands.guilds(_G)
async def report_prefs_show_cmd(i: discord.Interaction):
    p = await prefs_get(i.guild.id, i.user.id)
    def _fmt(mins: int) -> str: return f"{mins//60:02d}:{mins%60:02d}"
    await i.response.send_message(
        f"enabled={bool(p['enabled'])}\n"
        f"tz={p['tz']}\n"
        f"freq={p['freq_secs']//60} minutes\n"
        f"window={_fmt(p['window_start_min'])}-{_fmt(p['window_end_min'])}",
        ephemeral=True
    )

@bot.tree.command(name="report_prefs_disable", description="Disable your private reports")
@app_commands.guilds(_G)
async def report_prefs_disable_cmd(i: discord.Interaction):
    p = await prefs_get(i.guild.id, i.user.id)
    await prefs_set(i.guild.id, i.user.id, p["tz"], p["freq_secs"], p["window_start_min"], p["window_end_min"], 0)
    await i.response.send_message("Disabled.", ephemeral=True)

@bot.tree.command(name="report_prefs_enable", description="Enable your private reports")
@app_commands.guilds(_G)
async def report_prefs_enable_cmd(i: discord.Interaction):
    p = await prefs_get(i.guild.id, i.user.id)
    await prefs_set(i.guild.id, i.user.id, p["tz"], p["freq_secs"], p["window_start_min"], p["window_end_min"], 1)
    await i.response.send_message("Enabled.", ephemeral=True)

# --- Immediate server report (legacy helper; will read file if present) ---
@bot.tree.command(name="report_now", description="Post immediate server report")
@app_commands.guilds(_G)
async def report_now_cmd(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True, thinking=True)
    ch = bot.get_channel(REPORT_CHANNEL_ID) if REPORT_CHANNEL_ID else None
    if ch is None:
        await interaction.followup.send("Report channel unset.", ephemeral=True); return
    txt = await asyncio.to_thread(read_text_or_none, REPORT_SERVER_FILE)
    if not txt:
        await interaction.followup.send("No server report file found.", ephemeral=True); return
    await safe_send(ch, txt)
    await prune_channel_reports(ch, keep=3)
    await interaction.followup.send("Posted.", ephemeral=True)

# --- Roles ---
@bot.tree.command(name="subscribe", description="Subscribe to market alerts role")
@app_commands.guilds(_G)
async def subscribe_cmd(interaction: discord.Interaction):
    role = interaction.guild.get_role(MARKET_ALERT_ROLE_ID)
    if not role:
        await interaction.response.send_message("Alert role missing.", ephemeral=True); return
    try:
        await interaction.user.add_roles(role, reason="subscribe")
        await interaction.response.send_message("Subscribed.", ephemeral=True)
    except discord.Forbidden:
        await interaction.response.send_message("Missing permissions.", ephemeral=True)

@bot.tree.command(name="unsubscribe", description="Unsubscribe from market alerts role")
@app_commands.guilds(_G)
async def unsubscribe_cmd(interaction: discord.Interaction):
    role = interaction.guild.get_role(MARKET_ALERT_ROLE_ID)
    if not role:
        await interaction.response.send_message("Alert role missing.", ephemeral=True); return
    try:
        await interaction.user.remove_roles(role, reason="unsubscribe")
        await interaction.response.send_message("Unsubscribed.", ephemeral=True)
    except discord.Forbidden:
        await interaction.response.send_message("Missing permissions.", ephemeral=True)

@bot.tree.command(name="health", description="Show bot health")
@app_commands.guilds(_G)
async def health_cmd(interaction: discord.Interaction):
    await interaction.response.send_message(
        f"ok {now_iso()} | source={MARKET_SOURCE} | polling={MARKET_POLL_SECS}s | private={USER_REPORT_SECS}s",
        ephemeral=True
    )

# --- Owner-only Admin / Debug ---
def _owner_guard(i: discord.Interaction) -> bool:
    return is_owner(i.user)

@bot.tree.command(name="admin_delete_all", description="Owner: delete all messages in a channel")
@app_commands.guilds(_G)
@app_commands.describe(channel="Target text channel. Defaults to the current channel.")
async def admin_delete_all(i: discord.Interaction, channel: Optional[discord.TextChannel] = None):
    if not _owner_guard(i):
        await i.response.send_message("Not authorized.", ephemeral=True); return

    ch = channel or (i.channel if isinstance(i.channel, discord.TextChannel) else None)
    if ch is None:
        await i.response.send_message("This is not a text channel.", ephemeral=True); return

    me = i.guild.me
    perms = ch.permissions_for(me)
    if not (perms.manage_messages and perms.read_message_history and perms.view_channel):
        await i.response.send_message("Missing permissions: need Manage Messages, View Channel, and Read Message History.", ephemeral=True)
        return

    await i.response.defer(ephemeral=True, thinking=True)

    deleted_total = 0
    try:
        while True:
            batch = await ch.purge(limit=1000, bulk=True, check=lambda m: True, oldest_first=False)
            deleted_total += len(batch)
            if len(batch) == 0:
                break
    except Exception as e:
        log.error(f"/admin_delete_all bulk purge error: {e}")

    try:
        async for msg in ch.history(limit=None, oldest_first=True):
            try:
                await msg.delete()
                deleted_total += 1
            except Exception:
                pass
            await asyncio.sleep(0.2)
    except Exception as e:
        log.error(f"/admin_delete_all history delete error: {e}")

    await i.followup.send(f"Done. Deleted ≈ {deleted_total} messages in {ch.mention}.", ephemeral=True)

@bot.tree.command(name="admin_release_lock", description="Owner: release a user's active lock")
@app_commands.guilds(_G)
@app_commands.describe(member="Target member to release lock for")
async def admin_release_lock(i: discord.Interaction, member: discord.Member):
    if not _owner_guard(i):
        await i.response.send_message("Not authorized.", ephemeral=True); return
    await release_user_lock(i.guild.id, member.id)
    await i.response.send_message(f"Released lock for {member.mention}.", ephemeral=True)

@bot.tree.command(name="admin_resync", description="Owner: resync slash commands")
@app_commands.guilds(_G)
async def admin_resync(i: discord.Interaction):
    if not _owner_guard(i):
        await i.response.send_message("Not authorized.", ephemeral=True); return
    try:
        await sync_slash_clean()
        await i.response.send_message("Resynced.", ephemeral=True)
    except Exception as e:
        await i.response.send_message(f"error: {e}", ephemeral=True)

@bot.tree.command(name="admin_diag_counts", description="Owner: DB row counts")
@app_commands.guilds(_G)
async def admin_diag_counts(i: discord.Interaction):
    if not _owner_guard(i):
        await i.response.send_message("Not authorized.", ephemeral=True); return
    assert db is not None
    s1 = await (await db.execute("SELECT COUNT(*) FROM server_watchlist")).fetchone()
    s2 = await (await db.execute("SELECT COUNT(*) FROM user_watchlist")).fetchone()
    u1 = await (await userdb.execute("SELECT COUNT(*) FROM users")).fetchone()
    await i.response.send_message(f"server_watchlist={s1[0]}  user_watchlist={s2[0]}  users={u1[0]}", ephemeral=True)

@bot.tree.command(name="admin_export_watch", description="Owner: export watchlists as JSON")
@app_commands.guilds(_G)
async def admin_export_watch(i: discord.Interaction):
    if not _owner_guard(i):
        await i.response.send_message("Not authorized.", ephemeral=True); return
    assert db is not None
    out: Dict[str, Any] = {"server": {}, "users": {}}
    async with db.execute("SELECT guild_id,ticker,exchange FROM server_watchlist") as cur:
        async for gid, t, ex in cur:
            out["server"].setdefault(str(gid), []).append({"ticker": t, "exchange": _ex_out(ex) or "ALL"})
    async with db.execute("SELECT guild_id,user_id,ticker,exchange FROM user_watchlist") as cur:
        async for gid, uid, t, ex in cur:
            out["users"].setdefault(f"{gid}:{uid}", []).append({"ticker": t, "exchange": _ex_out(ex) or "ALL"})
    txt = json.dumps(out, indent=2)[:1900]
    await i.response.send_message(f"```json\n{txt}\n```", ephemeral=True)

@bot.tree.command(name="admin_sql", description="Owner: read-only SELECT on a DB")
@app_commands.guilds(_G)
@app_commands.describe(target="discord|user", sql="SELECT ...")
async def admin_sql(i: discord.Interaction, target: str, sql: str):
    if not _owner_guard(i):
        await i.response.send_message("Not authorized.", ephemeral=True); return
    target = target.lower().strip()
    if not sql.strip().lower().startswith("select"):
        await i.response.send_message("Only SELECT allowed.", ephemeral=True); return
    conn = db if target == "discord" else userdb if target == "user" else None
    if conn is None:
        await i.response.send_message("target must be 'discord' or 'user'.", ephemeral=True); return
    try:
        cur = await conn.execute(sql)
        rows = await cur.fetchall()
        head = []
        if rows:
            cols = [d[0] for d in cur.description]
            head.append(" | ".join(cols))
            for r in rows[:20]:
                head.append(" | ".join(str(x) for x in r))
        txt = "\n".join(head) if head else "(no rows)"
        await i.response.send_message(f"```\n{txt[:1800]}\n```", ephemeral=True)
    except Exception as e:
        await i.response.send_message(f"error: {e}", ephemeral=True)

@bot.tree.command(name="admin_echo", description="Owner: echo a message to a channel")
@app_commands.guilds(_G)
@app_commands.describe(channel="Target text channel", text="Message")
async def admin_echo(i: discord.Interaction, channel: discord.TextChannel, text: str):
    if not _owner_guard(i):
        await i.response.send_message("Not authorized.", ephemeral=True); return
    await safe_send(channel, text)
    await i.response.send_message("Sent.", ephemeral=True)

@bot.tree.command(name="admin_list_cmds", description="Owner: list slash commands visible in this guild")
@app_commands.guilds(_G)
async def admin_list_cmds(i: discord.Interaction):
    if not _owner_guard(i):
        await i.response.send_message("Not authorized.", ephemeral=True); return
    cmds = await bot.tree.fetch_commands(guild=i.guild)
    lines = [f"/{c.name} — {c.description or '-'}" for c in cmds]
    out = "\n".join(lines) or "(none)"
    await i.response.send_message(f"**Commands in this guild**\n{out[:1800]}", ephemeral=True)

# ========= main =========
async def start_http_server() -> web.AppRunner:  # redefined above; kept for clarity in main import order
    app = web.Application()
    app.add_routes([web.get("/health", handle_health), web.post("/", handle_incoming)])
    runner = web.AppRunner(app); await runner.setup()
    site = web.TCPSite(runner, INBOUND_HOST, INBOUND_PORT); await site.start()
    log.info(f"inbound HTTP listening on http://{INBOUND_PORT}/")
    return runner

async def main():
    if not DISCORD_TOKEN:
        raise RuntimeError("DISCORD_TOKEN is not set")
    global http_session, db, userdb
    http_session = aiohttp.ClientSession()
    db = await aiosqlite.connect(BOT_DB)
    await db.execute("PRAGMA busy_timeout=20000")
    await db_init()

    userdb = await aiosqlite.connect(USER_DB)
    await userdb.execute("PRAGMA busy_timeout=20000")
    await userdb_init()
    http_runner = await start_http_server()

    stop_event = asyncio.Event()
    def _signal_handler(*_): stop_event.set()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try: loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError: pass

    bot_task = asyncio.create_task(bot.start(DISCORD_TOKEN))
    await stop_event.wait()

    try: await bot.close()
    except Exception: pass
    try: await bot_task
    except Exception: pass
    try: await http_runner.cleanup()
    except Exception: pass
    try:
        await http_session.close()
        await db.close()
        await userdb.close()
    except Exception: pass

if __name__ == "__main__":
    asyncio.run(main())
