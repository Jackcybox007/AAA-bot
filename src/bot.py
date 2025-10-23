"""
PrUn Discord Market Hub Bot

A guild-scoped Discord bot for market data monitoring and analysis.

Features:
    - Slash commands (guild-scoped only)
    - Webhook bridge to n8n for AI chat integration
    - Periodic market snapshots (server and private watchlists)
    - Per-user FNAR CSV key registration
    - Private channel management
    - Market data analysis and reporting
    - Owner-only admin commands

Usage:
    python bot.py
"""

# Standard library imports
import asyncio
import hashlib
import json
import logging
import os
import signal
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

# Third-party imports
import aiohttp
import aiosqlite
import discord
from aiohttp import web
from discord import app_commands, Object as _Obj
from discord.ext import commands, tasks
from dotenv import load_dotenv

# Local imports
from config import config

# ========= env / config =========
load_dotenv()

# Use centralized configuration
DISCORD_TOKEN = config.DISCORD_TOKEN
MARKET_SOURCE = config.MARKET_SOURCE
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
REPORT_CHANNEL_ID = config.REPORT_CHANNEL_ID
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

bot = commands.Bot(command_prefix="!", intents=intents)
http_session: Optional[aiohttp.ClientSession] = None

# DB connections
db: Optional[aiosqlite.Connection] = None
userdb: Optional[aiosqlite.Connection] = None

# in-memory
_watch_rules: List[Dict[str, Any]] = []
_last_user_digest: Dict[Tuple[int, int], str] = {}
_start_time = datetime.now(timezone.utc)

# ========= utils =========
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

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
    await db.executescript(SCHEMA_SQL); await db.commit()

async def userdb_init():
    assert userdb is not None
    await userdb.executescript(USER_SCHEMA_SQL); await userdb.commit()

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

# user.db helpers
async def user_set_key(discord_id: int, key: str):
    assert userdb is not None
    ts = int(datetime.now(timezone.utc).timestamp())
    await userdb.execute(
        "INSERT INTO users(discord_id,fio_api_key,username,created_at,updated_at) VALUES(?,?,?,?,?) "
        "ON CONFLICT(discord_id) DO UPDATE SET fio_api_key=excluded.fio_api_key, updated_at=excluded.updated_at",
        (str(discord_id), key, None, ts, ts)
    ); await userdb.commit()
    msg = f"user_key UPSERT discord_id={discord_id} key_len={len(key)}"
    log.info(msg); dbops.info(msg)

async def user_get_key(discord_id: int) -> Optional[str]:
    assert userdb is not None
    cur = await userdb.execute("SELECT fio_api_key FROM users WHERE discord_id=?", (str(discord_id),))
    row = await cur.fetchone()
    val = row[0] if row else None
    dbops.info(f"user_key GET discord_id={discord_id} exists={bool(val)}")
    return val

async def user_upsert(discord_id: int, fio_api_key: Optional[str], username: Optional[str]):
    """Upsert FNAR key and optional in-game username."""
    assert userdb is not None
    ts = int(datetime.now(timezone.utc).timestamp())
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

# ========= slash sync cleanup (dedupe) =========
async def sync_slash_clean():
    """Clear global commands then sync to the target guild to prevent duplicates."""
    # Clear all GLOBAL commands
    bot.tree.clear_commands(guild=None)
    await bot.tree.sync(guild=None)      # push empty global set
    # Sync to one guild only
    synced = await bot.tree.sync(guild=_G)
    log.info(f"slash synced guild={TARGET_GUILD_ID} count={len(synced)}")

# ========= market I/O (PRUN DB or n8n) =========
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
                bid_qty = ask_qty = None
                if bb is not None:
                    r2 = await (await con.execute(
                        "SELECT SUM(qty) q FROM books WHERE cx=? AND ticker=? AND side='bid' AND price=?",
                        (cx, t, bb)
                    )).fetchone()
                    bid_qty = int(r2["q"] or 0)
                if ba is not None:
                    r3 = await (await con.execute(
                        "SELECT SUM(qty) q FROM books WHERE cx=? AND ticker=? AND side='ask' AND price=?",
                        (cx, t, ba)
                    )).fetchone()
                    ask_qty = int(r3["q"] or 0)
                out[t][cx] = {"bid": bb, "ask": ba, "bid_qty": bid_qty, "ask_qty": ask_qty, "ts": ts_val}
        return out

async def market_snapshot(items: List[Tuple[str, Optional[str]]]) -> Dict[str, Dict[str, Dict[str, Any]]]:
    if MARKET_SOURCE == "n8n":
        tickers = sorted({t.upper() for t, _ in items})
        explicit_ex = sorted({(ex or "").upper() for _, ex in items if ex})
        exchanges = (explicit_ex if explicit_ex else DEFAULT_EXCHANGES)
        return await n8n_snapshot(tickers, exchanges) or {}
    return await db_snapshot(items)

# ========= report helpers =========
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

def _digest_lines(lines: List[str]) -> str:
    h = hashlib.sha256()
    for ln in lines:
        h.update(ln.encode("utf-8")); h.update(b"\n")
    return h.hexdigest()

# ========= loops =========
@tasks.loop(seconds=MARKET_POLL_SECS)
async def market_report_loop():
    if not REPORT_CHANNEL_ID:
        return
    try:
        ch = bot.get_channel(REPORT_CHANNEL_ID)
        if ch is None:
            return

        items: List[Tuple[str, Optional[str]]] = []
        for g in bot.guilds:
            if g.id != TARGET_GUILD_ID:
                continue
            items.extend(await db_get_server_watch(g.id))
        log.info(f"report_watch_items={len(items)}")
        if not items:
            return

        snap = await market_snapshot(items)
        if not snap:
            if MARKET_SOURCE == "n8n" and N8N_MARKET_REPORT_URL:
                try:
                    assert http_session is not None
                    async with http_session.post(N8N_MARKET_REPORT_URL, json={"action": "report"}, timeout=180) as r:
                        t = await r.text()
                        log.info(f"n8n text report status={r.status} bytes={len(t)}")
                        if r.status == 200 and t:
                            await safe_send(ch, t)
                except Exception as e:
                    log.error(f"n8n text report error: {e}")
            return

        lines, arbs = [], []
        for tck, rows in snap.items():
            a = best_arbitrage_for_ticker(rows)
            if a and a[4] >= ARBITRAGE_MIN_SPREAD_PCT:
                buy_cx, sell_cx, bp, sp, pct = a
                arbs.append(f"{tck}: buy {buy_cx} {bp:.1f} → sell {sell_cx} {sp:.1f} (+{pct:.2f}%)")
            parts = []
            for cx in sorted(rows.keys()):
                v = rows[cx]
                parts.append(f"{cx} b{v.get('bid','?')} a{v.get('ask','?')}")
            lines.append(f"{tck}: " + " | ".join(parts))

        embed = discord.Embed(
            title="Market Snapshot",
            description="\n".join(lines[:15]) if lines else "No data",
            timestamp=datetime.now(timezone.utc),
            colour=discord.Colour.blue(),
        )
        if arbs:
            embed.add_field(name="Arbitrage candidates", value="\n".join(arbs[:10]), inline=False)
        embed.set_footer(text=f"Source: {MARKET_SOURCE.upper()}")
        await ch.send(embed=embed)
    except Exception as e:
        log.error(f"market_report_loop error: {e}")

@market_report_loop.before_loop
async def before_market_report():
    await bot.wait_until_ready()

@tasks.loop(seconds=USER_REPORT_SECS)
async def user_private_loop():
    if MARKET_SOURCE not in ("n8n", "db"):
        return
    try:
        for g in bot.guilds:
            if g.id != TARGET_GUILD_ID:
                continue
            user_items: Dict[int, List[Tuple[str, Optional[str]]]] = {}
            async with db.execute("SELECT user_id,ticker,exchange FROM user_watchlist WHERE guild_id=?", (g.id,)) as cur:
                async for row in cur:
                    uid, t, ex = int(row[0]), row[1], _ex_out(row[2])
                    user_items.setdefault(uid, []).append((t, ex))
            if not user_items:
                continue

            union: List[Tuple[str, Optional[str]]] = []
            for lst in user_items.values():
                union.extend(lst)
            snap = await market_snapshot(union)
            if not snap:
                continue

            for uid, lst in user_items.items():
                m = g.get_member(uid)
                if not m or m.bot:
                    continue
                ch_id = await db_get_private_channel(g.id, uid)
                ch = g.get_channel(ch_id) if ch_id else None
                if ch is None:
                    ch = await ensure_member_private_channel(g, m)
                    if ch:
                        await db_cache_private_channel(g.id, uid, ch.id)
                if ch is None:
                    continue

                wanted = {(t.upper(), (ex.upper() if ex else None)) for (t, ex) in lst}
                lines: List[str] = []
                for tck, rows in snap.items():
                    if (tck.upper(), None) in wanted or any((tck.upper(), k.upper()) in wanted for k in rows.keys()):
                        parts = []
                        for cx in sorted(rows.keys()):
                            v = rows[cx]
                            parts.append(f"{cx} b{v.get('bid','?')} a{v.get('ask','?')}")
                        lines.append(f"{tck}: " + " | ".join(parts))
                if not lines:
                    continue

                dg = _digest_lines(lines)
                key = (g.id, uid)
                if _last_user_digest.get(key) == dg:
                    continue
                _last_user_digest[key] = dg

                embed = discord.Embed(
                    title="Your Watchlist Snapshot",
                    description="\n".join(lines[:15]),
                    timestamp=datetime.now(timezone.utc),
                    colour=discord.Colour.green(),
                )
                embed.set_footer(text=f"Source: {MARKET_SOURCE.upper()}")
                try:
                    await ch.send(embed=embed)
                except Exception as e:
                    log.error(f"user_private send error uid={uid}: {e}")
    except Exception as e:
        log.error(f"user_private_loop error: {e}")

@user_private_loop.before_loop
async def before_user_private():
    await bot.wait_until_ready()

# ========= HTTP inbound bridge =========
async def handle_health(request: web.Request) -> web.StreamResponse:
    return web.json_response({"ok": True, "service": "prun-discord-bridge", "time": now_iso(), "source": MARKET_SOURCE})

async def handle_incoming(request: web.Request) -> web.StreamResponse:
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
    log.info(f"inbound msg len={len(msg_text)} ch={channel_id} srv={server_id} snip='{_snippet(msg_text)}'")

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
        return web.json_response({"ok": False, "error": "channel_not_found"}, status=404)

    try:
        if len(msg_text) > MAX_DISCORD_MSG_LEN:
            chunks = chunk_text(msg_text, MAX_DISCORD_MSG_LEN)
            log.info(f"inbound chunking parts={len(chunks)}")
            for c in chunks:
                await channel.send(c)
        else:
            await channel.send(msg_text)
        return web.json_response({"ok": True})
    except Exception as e:
        log.error(f"inbound send failed: {e}")
        return web.json_response({"ok": False, "error": str(e)}, status=500)

async def start_http_server() -> web.AppRunner:
    app = web.Application()
    app.add_routes([web.get("/health", handle_health), web.post("/", handle_incoming)])
    runner = web.AppRunner(app); await runner.setup()
    site = web.TCPSite(runner, INBOUND_HOST, INBOUND_PORT); await site.start()
    log.info(f"inbound HTTP listening on http://{INBOUND_PORT}/")
    return runner

# ========= events =========
@bot.event
async def on_ready():
    log.info(f"ready as {bot.user} id={bot.user.id} guilds={len(bot.guilds)} members={sum(len(g.members) for g in bot.guilds)}")
    if ERROR_LOG_CHANNEL_ID:
        h = DiscordErrorHandler(bot, ERROR_LOG_CHANNEL_ID)
        h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
        logging.getLogger().addHandler(h)

    await load_watch_rules()

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

    try:
        await sync_slash_clean()
    except Exception as e:
        log.error(f"slash sync failed: {e}")

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
            await ch.send(f"Hi {member.mention}. This is your private channel with the bot.")
        log.info(f"member_join uid={member.id} guild={member.guild.id} private_ch={(ch.id if ch else None)}")
    except Exception as e:
        log.error(f"on_member_join error: {e}")

@bot.event
async def on_message(message: discord.Message):
    if message.author == bot.user or getattr(message.author, "bot", False):
        return
    if not message.guild or message.guild.id != TARGET_GUILD_ID:
        return
    log.debug(f"msg uid={message.author.id} ch={message.channel.id} guild={message.guild.id} len={len(message.content)} excluded={is_excluded(message.channel)}")
    if not is_excluded(message.channel):
        payload = {
            "message": message.content or "",
            "channelID": str(message.channel.id),
            "serverID": str(message.guild.id),
            "discordID": str(message.author.id),
        }
        if message.attachments and not payload["message"]:
            payload["message"] = " ".join(a.url for a in message.attachments)
        try:
            assert http_session is not None
            async with http_session.post(N8N_WEBHOOK_URL, json=payload, timeout=180) as resp:
                txt = await resp.text()
                log.info(f"webhook tx status={resp.status} req_len={len(payload['message'])} resp_bytes={len(txt)}")
                if resp.status == 200:
                    try:
                        data = await resp.json()
                        reply = data.get("message") or data.get("reply")
                        if reply:
                            await safe_send(message.channel, reply)
                    except Exception:
                        if txt:
                            await safe_send(message.channel, txt)
                else:
                    log.error(f"webhook HTTP {resp.status}: {_snippet(txt)}")
        except Exception as e:
            log.error(f"webhook exception: {e}")
    await bot.process_commands(message)

# ========= slash commands (guild-scoped) =========
# --- Info / QoL ---
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
    # persist key and optional username
    await user_upsert(interaction.user.id, fio_api_key, username)

    # ensure private channel exists and is cached
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

# --- Reports ---
@bot.tree.command(name="report_now", description="Post immediate market snapshot")
@app_commands.guilds(_G)
async def report_now_cmd(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True, thinking=True)
    ch = bot.get_channel(REPORT_CHANNEL_ID) if REPORT_CHANNEL_ID else None
    if ch is None:
        await interaction.followup.send("Report channel unset.", ephemeral=True); return

    items: List[Tuple[str, Optional[str]]] = []
    for g in bot.guilds:
        if g.id != TARGET_GUILD_ID:
            continue
        items.extend(await db_get_server_watch(g.id))
    if MARKET_SOURCE == "n8n" and N8N_MARKET_REPORT_URL:
        try:
            assert http_session is not None
            req = {"action": "report", "tickers": sorted({t for t,_ in items}), "exchanges": DEFAULT_EXCHANGES}
            async with http_session.post(N8N_MARKET_REPORT_URL, json=req, timeout=180) as resp:
                t = await resp.text()
                log.info(f"n8n report_now status={resp.status} bytes={len(t)}")
                if resp.status == 200:
                    try:
                        data = await resp.json()
                        if isinstance(data, dict) and data.get("message"):
                            await safe_send(ch, data["message"])
                        else:
                            await safe_send(ch, t if t else "OK")
                    except Exception:
                        await safe_send(ch, t if t else "OK")
                    await interaction.followup.send("Posted.", ephemeral=True); return
                else:
                    await interaction.followup.send(f"n8n HTTP {resp.status}", ephemeral=True); return
        except Exception as e:
            log.error(f"n8n report_now error: {e}")
            await interaction.followup.send("n8n error.", ephemeral=True); return

    if not items:
        await interaction.followup.send("Watchlist empty.", ephemeral=True); return
    snap = await market_snapshot(items)
    if not snap:
        await interaction.followup.send("No data.", ephemeral=True); return

    lines, arbs = [], []
    for tck, rows in snap.items():
        a = best_arbitrage_for_ticker(rows)
        if a and a[4] >= ARBITRAGE_MIN_SPREAD_PCT:
            buy_cx, sell_cx, bp, sp, pct = a
            arbs.append(f"{tck}: buy {buy_cx} {bp:.1f} → sell {sell_cx} {sp:.1f} (+{pct:.2f}%)")
        parts = []
        for cx in sorted(rows.keys()):
            v = rows[cx]; parts.append(f"{cx} b{v.get('bid','?')} a{v.get('ask','?')}")
        lines.append(f"{tck}: " + " | ".join(parts))

    embed = discord.Embed(
        title="Market Snapshot",
        description="\n".join(lines[:15]) if lines else "No data",
        timestamp=datetime.now(timezone.utc),
        colour=discord.Colour.blue(),
    )
    if arbs:
        embed.add_field(name="Arbitrage candidates", value="\n".join(arbs[:10]), inline=False)
    embed.set_footer(text=f"Source: {MARKET_SOURCE.upper()}")
    await ch.send(embed=embed)
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

@bot.tree.command(name="resync", description="Owner: prune duplicates and resync slash commands")
@app_commands.guilds(_G)
async def resync_cmd(i: discord.Interaction):
    if not _owner_guard(i):
        await i.response.send_message("Not authorized.", ephemeral=True); return
    try:
        await sync_slash_clean()
        await i.response.send_message("Resynced.", ephemeral=True)
    except Exception as e:
        await i.response.send_message(f"error: {e}", ephemeral=True)

@bot.tree.command(name="admin_set_report_channel", description="Owner: set report channel")
@app_commands.guilds(_G)
@app_commands.describe(channel="Target text channel")
async def admin_set_report_channel(i: discord.Interaction, channel: discord.TextChannel):
    if not _owner_guard(i):
        await i.response.send_message("Not authorized.", ephemeral=True); return
    global REPORT_CHANNEL_ID
    REPORT_CHANNEL_ID = int(channel.id)
    await i.response.send_message(f"REPORT_CHANNEL_ID set to {REPORT_CHANNEL_ID}.", ephemeral=True)

@bot.tree.command(name="admin_set_error_channel", description="Owner: set error log channel")
@app_commands.guilds(_G)
@app_commands.describe(channel="Target text channel")
async def admin_set_error_channel(i: discord.Interaction, channel: discord.TextChannel):
    if not _owner_guard(i):
        await i.response.send_message("Not authorized.", ephemeral=True); return
    global ERROR_LOG_CHANNEL_ID
    ERROR_LOG_CHANNEL_ID = int(channel.id)
    await i.response.send_message(f"ERROR_LOG_CHANNEL_ID set to {ERROR_LOG_CHANNEL_ID}. Restart recommended.", ephemeral=True)

@bot.tree.command(name="admin_force_private", description="Owner: create/ensure a private channel for a member")
@app_commands.guilds(_G)
@app_commands.describe(member="Target member")
async def admin_force_private(i: discord.Interaction, member: discord.Member):
    if not _owner_guard(i):
        await i.response.send_message("Not authorized.", ephemeral=True); return
    ch = await ensure_member_private_channel(i.guild, member)
    if ch and db:
        await db_cache_private_channel(i.guild.id, member.id, ch.id)
    await i.response.send_message(f"Private channel: {ch.mention if ch else 'failed'}", ephemeral=True)

@bot.tree.command(name="admin_sync", description="Owner: force slash sync (guild only)")
@app_commands.guilds(_G)
async def admin_sync(i: discord.Interaction):
    if not _owner_guard(i):
        await i.response.send_message("Not authorized.", ephemeral=True); return
    try:
        await sync_slash_clean()
        await i.response.send_message("Synced.", ephemeral=True)
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
async def main():
    if not DISCORD_TOKEN:
        raise RuntimeError("DISCORD_TOKEN is not set")
    global http_session, db, userdb
    http_session = aiohttp.ClientSession()
    db = await aiosqlite.connect(BOT_DB); await db_init()
    userdb = await aiosqlite.connect(USER_DB); await userdb_init()
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
