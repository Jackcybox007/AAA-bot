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

