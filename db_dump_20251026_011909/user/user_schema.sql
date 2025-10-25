-- table users
CREATE TABLE users(
  discord_id TEXT PRIMARY KEY,
  fio_api_key TEXT NOT NULL,
  username TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);

