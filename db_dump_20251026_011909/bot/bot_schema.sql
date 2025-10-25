-- table server_watchlist
CREATE TABLE server_watchlist(
  guild_id INTEGER NOT NULL,
  ticker   TEXT NOT NULL,
  exchange TEXT NOT NULL DEFAULT '',
  PRIMARY KEY(guild_id, ticker, exchange)
);

-- table user_meta
CREATE TABLE user_meta(
  guild_id INTEGER NOT NULL,
  user_id  INTEGER NOT NULL,
  private_channel_id INTEGER, total_public_msgs INTEGER NOT NULL DEFAULT 0, total_private_msgs INTEGER NOT NULL DEFAULT 0, active_lock INTEGER NOT NULL DEFAULT 0, lock_acquired_at INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY(guild_id, user_id)
);

-- table user_watchlist
CREATE TABLE user_watchlist(
  guild_id INTEGER NOT NULL,
  user_id  INTEGER NOT NULL,
  ticker   TEXT NOT NULL,
  exchange TEXT NOT NULL DEFAULT '',
  PRIMARY KEY(guild_id, user_id, ticker, exchange)
);

