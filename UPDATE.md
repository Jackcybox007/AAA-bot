**🚀 AXO Market Sentinel — Update Log (24/10/2025)**

**What’s New**

1. **Request Lock System**
   Each user can now have **only one active request** at a time.
   This prevents spam or overlapping queries and keeps responses clean and fast.
   The lock automatically releases once the bot replies — or after 15 minutes if stuck.

2. **Per-Channel Memory (Conversation Mode)**
   The bot now **remembers the last 5 messages** per channel.
   You can have short ongoing discussions about market data — no need to restate context every time.
   Example:

> `?best RAT @ CI1`
> `?and compare with NC1`
> → The bot will understand both.

**Notes**

* Request now use `?` as the prefix (e.g., `?best`, `?arb`, `?spread`).
* If the bot doesn’t respond, wait a bit — your request may be locked by an ongoing reply.

**🚀 AXO Market Sentinel — Update Log (25/10/2025)**

**What’s New**

1. **LM Ship Ingestion**
   Added `/csv/localmarket/ship/{planetID}` with the new header:
   `ContractNaturalId, PlanetNaturalId, PlanetName, OriginPlanetNaturalId, OriginPlanetName, DestinationPlanetNaturalId, DestinationPlanetName, CargoWeight, CargoVolume, CreatorCompanyName, CreatorCompanyCode, PayoutPrice, PayoutCurrency, DeliveryTime, CreationTimeEpochMs, ExpiryTimeEpochMs, MinimumRating`.

2. **LM Buy/Sell Improvements**
   Uses only `/buy`, `/sell`, `/ship` endpoints. No `/bu` fallback.
   Planet scan now also includes fixed hubs: **MOR, HRT, ANT, BEN, HUB, ARC**.

3. **New Tools for LLM**

   * `lm_arbitrage_near_cx`: finds LM sells below CX ask and LM buys above CX bid near selected planets.
   * `mats_search`: fast material search by ticker or name.

4. **Private DB Schema**
   `user_inventory`, `user_cxos`, `user_balances` now keyed by **discord_id**.
   Auto-recreate on schema mismatch to prevent silent failures.

5. **System Prompt Update**
   Refreshed identity, routing hints, data pipeline, and the new LM tools so replies stay consistent and precise.

**Fixes**

* Resolved `NOT NULL constraint failed: user_inventory.discord_id` by writing `discord_id` on inserts and keys.
* Fixed SQLite `near "PRIMARY": syntax error` by switching to explicit per-table DDL.

**Notes**

* Edges are raw vs CX (no fees).
* Filter `lm_arbitrage_near_cx` by `planet_ids_csv`, `tickers_csv`, `currency`, and `min_edge_pct`.
* LM data lands in `LM_buy`, `LM_sell`, `LM_ship`; CX in `prices` and `price_history`.

**🚀 AXO Market Sentinel — Update Log (26/10/2025)**

**What’s New**

1. **Market reports pipeline**
   - Three reports: **universe**, **server watchlist**, **user watchlist**.
   - Output paths: `tmp/reports/universe/report.md`, `tmp/reports/server/report.md`, `tmp/reports/users/<user_id>/report.md`.
   - Universe keeps the narrative format (no tables). Server/user use the Discord style you specified.

2. **Market stats store**
   - New `market_stats` table in `prun.db` with categories:
     `top_movers`, `best_pct_spread`, `arb_margin`, `maker_profit` (top-50 each per run).

3. **Price model upgrades**
   - `prices` now includes **PP7** and **PP30**.
   - `prices_chart_history` ingests **CXPC** JSON from `rest.fnar.net` with
     `MINUTE_FIVE` (7-day retention) and `HOUR_TWO` (30-day retention).
   - Standard CX set: **NC1, NC2, CI1, CI2, IC1, AI1, UN**.

4. **Performance**
   - High-concurrency HTTP (up to 1000), batch upserts, and conditional GET via `http_cache` (ETag/Last-Modified).
   - Parallel LM and CXPC fetchers with tunable semaphores.

5. **Discord bot**
   - Reads server/user reports from `tmp/reports/*`.
   - Posts server report to **#market-report** and keeps only the **last 3** bot messages.
   - Sends user reports to each user’s private channel.
   - New commands:
     - `/report_prefs_set tz every_minutes window_start window_end`
     - `/report_prefs_show`
     - `/report_prefs_enable`
     - `/report_prefs_disable`

6. **MCP tools**
   - `market_report(kind=universe|server|user, user_id?)` returns rendered markdown.
   - `cx_best_spreads(cx, order=tightest|widest, limit)` computes spread% per ticker:
     `((ask − bid) / ((bid + ask)/2)) * 100`.

7. **System prompt**
   - Updated with new tools, routing, and execution pipeline.

**Fixes**

- `market_snapshot` UPSERT now valid: UNIQUE on `(cx,ticker)` plus `ts_iso` column; backfill added.
- `init.py` provisions all new tables and indexes (`prices`, `price_history`, `prices_chart_history`, `http_cache`, `market_stats`, `market_snapshot`).

**Notes**

- Server/user reports are **watchlist-filtered**. Universe covers all tickers/CXs.
- Edges and spreads are **raw**; no fee model applied yet.

**🚀 AXO Market Sentinel — Update Log (28/10/2025)**

**What’s New**

1. **Targeted report updates**
   Server and user loops now call dedicated builders:
   `update_server_report()` and `update_user_report(user_id)`.
   Only the requested report is rebuilt.

2. **Change-only reporting**
   Server and user reports include **only entries whose bid/ask changed**
   since the last run. Noise reduced. Faster reading.

3. **Liquidity tools**
   - `volume_recent(cx,ticker,hours,interval?)`
   - `cx_volume_ranking(ticker,hours,interval?)`
   - `recommend_sell_cx(ticker,hours,bid_weight,vol_weight,interval?)`
   Use volume + bid to suggest where to sell faster.

4. **LM price-per-unit**
   `lm_search` and `lm_arbitrage_near_cx` now show **ppu = Price / Amount**.

5. **Users & factions**
   - `faction_player_counts(country_code?, username?, limit?)` from `./tmp/user.json`
   - `users_info_search(query, limit?)` → planets, sub plan, ratings, corp, HQ level, company age

6. **System prompt**
   Updated with the new tools and routing hints.

**Fixes**

- Server/user posting respects digest gating and skips reposts when file unchanged.
- Report state tracked per scope to avoid cross-talk.

**Notes**

- Paths: `tmp/reports/server/report.md` and `tmp/reports/users/<id>/report.md`.
- CX edges remain **raw** (no fees).
<<<<<<< HEAD
- Discord channel keeps the **last 3** server reports.
=======
- Discord channel keeps the **last 3** server reports.

**🚀 AXO Market Sentinel — Update Log (02/11/2025)**

**What’s New**

1. **Order book ingestion**

* Pulls `/csv/bids` and `/csv/orders` (no key).
* Writes latest to `books` and 24h history to `books_hist`.
* Computes `level` per snapshot: bids by price desc, asks by price asc. Ties keep CSV order.
* Stores `company_id` as TEXT hash plus `company_name`, `company_code`.

2. **MCP tools**

* `ob_levels(cx,ticker,depth=20)` → aggregated top-N per side.
* `ob_imbalance(cx,ticker,depth=10)` → [-1, 1] using top-N.
* `ob_microprice(cx,ticker)` → size-weighted microprice from best bid/ask.
* `ob_support_resistance(cx,ticker,mode='book'|'history',cluster=1.0,top=3,lookback_h=168)`.

3. **@mention routing**

* Replies to n8n-sourced messages now prefix `<@discordId>` in public and private.

4. **Lock avoidance and timing**

* Staggered loops: report loops offset by `POLL_SECS/2` with ±10% jitter.
* Single-transaction batched upserts per poll.
* Readers use short-lived connections.

5. **Schema and indexes**

* `books` adds `level`, `company_id`, `company_name`, `company_code`.
* New `books_hist` with indexes on `(cx,ticker,side,ts)`, `(cx,ticker,side,level,ts)`, `(cx,ticker,side,price,ts)`.

**Fixes**

* Reduced `database is locked` in `market_report_loop` and `user_private_loop` via timing and PRAGMAs (`WAL`, `synchronous=NORMAL`, `busy_timeout=20000` on every connect).

**Notes**

* Imbalance default depth is 10; pass any depth as needed.
* S/R: `mode='book'` clusters by 1.0-credit bins; `mode='history'` uses swing highs/lows.
* Order-book history retention: 24h with rolling cleanup.

>>>>>>> 635973c (Implement some small fix with database and add order book related tools. #004)
