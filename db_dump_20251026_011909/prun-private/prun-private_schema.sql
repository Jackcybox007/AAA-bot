-- table user_balances
CREATE TABLE user_balances(
  discord_id TEXT NOT NULL,
  username   TEXT NOT NULL,
  currency   TEXT NOT NULL,
  amount     REAL NOT NULL,
  ts         INTEGER NOT NULL,
  PRIMARY KEY(discord_id, currency)
);

-- table user_cxos
CREATE TABLE user_cxos(
  discord_id       TEXT NOT NULL,
  username         TEXT NOT NULL,
  order_id         TEXT NOT NULL,
  exchange_code    TEXT,
  order_type       TEXT,
  material_ticker  TEXT,
  amount           REAL,
  initial_amount   REAL,
  price_limit      REAL,
  currency         TEXT,
  status           TEXT,
  created_epoch_ms INTEGER,
  ts               INTEGER NOT NULL,
  PRIMARY KEY(discord_id, order_id)
);

-- table user_inventory
CREATE TABLE user_inventory(
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

