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


