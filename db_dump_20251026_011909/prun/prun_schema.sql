-- index idx_assets_holdings_tb
CREATE INDEX idx_assets_holdings_tb ON assets_holdings(ticker,base);

-- index idx_planned_orders
CREATE INDEX idx_planned_orders ON planned_orders(status,cx,ticker,side);

-- index idx_snapshot_time
CREATE INDEX idx_snapshot_time ON market_snapshot(ts_ms);

-- index idx_stats_cat_time
CREATE INDEX idx_stats_cat_time ON market_stats(category, ts_ms);

-- index ix_books_cx_ticker
CREATE INDEX ix_books_cx_ticker ON books(cx, ticker);

-- index ix_books_side_price
CREATE INDEX ix_books_side_price ON books(side, price);

-- index ix_lm_buy_planet
CREATE INDEX ix_lm_buy_planet   ON LM_buy(PlanetNaturalId);

-- index ix_lm_buy_ticker
CREATE INDEX ix_lm_buy_ticker   ON LM_buy(MaterialTicker);

-- index ix_lm_sell_planet
CREATE INDEX ix_lm_sell_planet  ON LM_sell(PlanetNaturalId);

-- index ix_lm_sell_ticker
CREATE INDEX ix_lm_sell_ticker  ON LM_sell(MaterialTicker);

-- index ix_lm_ship_destination
CREATE INDEX ix_lm_ship_destination  ON LM_ship(DestinationPlanetNaturalId);

-- index ix_lm_ship_origin
CREATE INDEX ix_lm_ship_origin       ON LM_ship(OriginPlanetNaturalId);

-- index ix_lm_ship_planet
CREATE INDEX ix_lm_ship_planet       ON LM_ship(PlanetNaturalId);

-- index ix_pch_ts
CREATE INDEX ix_pch_ts ON prices_chart_history(ts);

-- index ix_price_history_ts
CREATE INDEX ix_price_history_ts ON price_history(ts);

-- index ux_market_snapshot_cx_ticker
CREATE UNIQUE INDEX ux_market_snapshot_cx_ticker ON market_snapshot(cx, ticker);

-- table LM_buy
CREATE TABLE LM_buy(
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

-- table LM_sell
CREATE TABLE LM_sell(
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

-- table LM_ship
CREATE TABLE LM_ship(
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

-- table assets_holdings
CREATE TABLE assets_holdings(
  id INTEGER PRIMARY KEY,
  ticker TEXT NOT NULL,
  base TEXT,
  qty REAL NOT NULL DEFAULT 0,
  avg_cost REAL NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL
);

-- table assets_txn
CREATE TABLE assets_txn(
  id INTEGER PRIMARY KEY,
  ticker TEXT NOT NULL,
  base TEXT,
  qty REAL NOT NULL,
  price REAL NOT NULL,
  at TEXT NOT NULL
);

-- table books
CREATE TABLE books(
  cx     TEXT NOT NULL,
  ticker TEXT NOT NULL,
  side   TEXT NOT NULL CHECK(side IN('bid','ask')),
  price  REAL NOT NULL,
  qty    REAL NOT NULL,
  ts     INTEGER NOT NULL,
  PRIMARY KEY(cx, ticker, side, price)
);

-- table http_cache
CREATE TABLE http_cache(
  url            TEXT PRIMARY KEY,
  etag           TEXT,
  last_modified  TEXT,
  fetched_ts     INTEGER
);

-- table market_snapshot
CREATE TABLE market_snapshot(
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

-- table market_stats
CREATE TABLE market_stats(
  ts_ms    INTEGER NOT NULL,
  category TEXT    NOT NULL,  -- 'top_movers','best_pct_spread','arb_margin','maker_profit'
  rank     INTEGER NOT NULL,
  cx       TEXT,
  ticker   TEXT,
  value    REAL,              -- main score (e.g., % or z or margin%)
  aux      REAL,              -- secondary (e.g., profit_per_unit)
  note     TEXT,              -- e.g., "NC1->IC1"
  PRIMARY KEY (ts_ms, category, rank, cx, ticker)
);

-- table materials
CREATE TABLE materials(
  ticker   TEXT PRIMARY KEY,
  name     TEXT,
  category TEXT,
  weight   REAL,
  volume   REAL
);

-- table planned_orders
CREATE TABLE planned_orders(
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

-- table price_history
CREATE TABLE price_history(
  cx        TEXT NOT NULL,
  ticker    TEXT NOT NULL,
  bid       REAL,
  ask       REAL,
  ts        INTEGER NOT NULL,
  PRIMARY KEY(cx, ticker, ts)
);

-- table prices
CREATE TABLE prices(
  cx        TEXT NOT NULL,
  ticker    TEXT NOT NULL,
  best_bid  REAL,
  best_ask  REAL,
  PP7       REAL,            -- rolling 7d mid
  PP30      REAL,            -- rolling 30d mid
  ts        INTEGER NOT NULL,
  PRIMARY KEY(cx, ticker)
);

-- table prices_chart_history
CREATE TABLE prices_chart_history(
  cx        TEXT    NOT NULL,
  ticker    TEXT    NOT NULL,
  mat_id    INTEGER NOT NULL,
  interval  TEXT    NOT NULL,   -- MINUTE_FIVE or HOUR_TWO
  ts        INTEGER NOT NULL,   -- DateEpochMs
  open      REAL,
  close     REAL,
  high      REAL,
  low       REAL,
  volume    REAL,
  traded    INTEGER,
  PRIMARY KEY(cx, ticker, interval, ts)
);

