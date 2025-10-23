#!/usr/bin/env python3
"""
PrUn Database Dumper

A utility to export SQLite databases to CSV and JSONL formats with schema.

Features:
    - Exports multiple SQLite databases
    - Generates schema SQL files
    - Exports tables and views to CSV/JSONL
    - Row count summaries
    - Configurable output options

Default Databases:
    - Discord.db (bot state)
    - prun-private.db (user data)
    - prun.db (market data)
    - user.db (user credentials)

Usage:
    python dump.py [options]
    
Options:
    --db PATH        Database file path (repeatable)
    --out DIR        Output directory
    --limit N        Row limit per table
    --no-views       Skip dumping views
    --no-jsonl       Skip JSONL output
"""

# Standard library imports
import argparse
import base64
import csv
import datetime as dt
import json
import logging
import os
import sqlite3
from pathlib import Path
from typing import Iterable, Tuple

# Local imports
from config import config

# -------- logging setup --------
# Setup logging using centralized config
log = config.setup_logging("dump")

# Default databases using new paths
DEFAULT_DBS = [
    os.path.join(config.DATABASE_DIR, "bot.db"),
    os.path.join(config.DATABASE_DIR, "prun-private.db"),
    os.path.join(config.DATABASE_DIR, "prun.db"),
    os.path.join(config.DATABASE_DIR, "user.db")
]

def to_serializable(v):
    if isinstance(v, bytes):
        return "base64:" + base64.b64encode(v).decode("ascii")
    return v

def qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'

def iter_rows(conn: sqlite3.Connection, sql: str, args: Tuple = ()):
    cur = conn.execute(sql, args)
    cols = [c[0] for c in cur.description]
    yield cols
    for row in cur:
        yield row

def write_csv(path: Path, header: Iterable[str], rows: Iterable[Iterable]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        for r in rows:
            w.writerow([to_serializable(x) for x in r])

def write_jsonl(path: Path, header: Iterable[str], rows: Iterable[Iterable]):
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = list(header)
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            obj = {cols[i]: to_serializable(r[i]) for i in range(len(cols))}
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")

def dump_db(db_path: Path, out_root: Path, limit: int = 0, include_views: bool = True, jsonl: bool = True):
    log.info(f"Processing database: {db_path}")
    print(f"==> {db_path}")
    if not db_path.exists():
        log.warning(f"Database not found: {db_path}")
        print(f"   ! not found, skipping")
        return

    db_out = out_root / db_path.stem
    db_out.mkdir(parents=True, exist_ok=True)

    uri = f"file:{db_path.as_posix()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, check_same_thread=False)
    conn.row_factory = sqlite3.Row

    # schema dump
    schema_file = db_out / f"{db_path.stem}_schema.sql"
    with schema_file.open("w", encoding="utf-8") as sf:
        for (name, type_, sql) in conn.execute(
            "SELECT name,type,sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY type,name"
        ):
            sf.write(f"-- {type_} {name}\n{sql};\n\n")

    # list objects
    obj_rows = list(conn.execute(
        "SELECT name,type FROM sqlite_master WHERE type IN ('table','view') "
        "AND name NOT LIKE 'sqlite_%' ORDER BY type,name"
    ))

    summary_lines = []
    for (name, type_) in obj_rows:
        is_view = (type_ == "view")
        if is_view and not include_views:
            continue

        out_dir = db_out / ("views" if is_view else "tables")
        out_dir.mkdir(parents=True, exist_ok=True)

        # build SELECT
        sql = f"SELECT * FROM {qident(name)}"
        count = None
        # count may be expensive for views; try and ignore failures
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {qident(name)}").fetchone()[0]
        except Exception:
            pass

        # header and rows
        cur = conn.execute(sql + (f" LIMIT {int(limit)}" if limit > 0 else ""))
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()

        # write CSV
        csv_path = out_dir / f"{name}.csv"
        write_csv(csv_path, cols, rows)

        # write JSONL (optional)
        if jsonl:
            jsonl_path = out_dir / f"{name}.jsonl"
            write_jsonl(jsonl_path, cols, rows)

        shown = len(rows)
        suffix = f" (limited to {shown})" if limit > 0 else f" ({shown} rows)"
        if count is not None and limit == 0:
            suffix = f" ({shown} rows, count={count})"
        summary_lines.append(f"{type_:5} {name}: {suffix}")
        print(f"   {type_:5} {name}: wrote {csv_path.name}{' + jsonl' if jsonl else ''}{suffix}")
        log.info(f"Exported {type_} {name}: {suffix}")

    # summary
    with (db_out / "_SUMMARY.txt").open("w", encoding="utf-8") as sf:
        sf.write("\n".join(summary_lines) + "\n")

    log.info(f"Completed database dump: {db_path} -> {db_out}")
    conn.close()

def main():
    ap = argparse.ArgumentParser(description="Dump SQLite databases to CSV/JSONL with schema.")
    ap.add_argument("--out", default=None, help="Output directory (default: db_dump_YYYYmmdd_HHMMSS)")
    ap.add_argument("--db", action="append", default=None,
                    help="Path to a SQLite DB. May be given multiple times. "
                         f"Default: {', '.join(DEFAULT_DBS)}")
    ap.add_argument("--limit", type=int, default=0, help="Row limit per table/view (0 = no limit).")
    ap.add_argument("--no-views", action="store_true", help="Skip dumping views.")
    ap.add_argument("--no-jsonl", action="store_true", help="Do not write JSONL, only CSV.")
    args = ap.parse_args()

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    outdir = Path(args.out or f"db_dump_{ts}")
    outdir.mkdir(parents=True, exist_ok=True)
    log.info(f"Starting database dump to: {outdir}")

    dbs = args.db if args.db else DEFAULT_DBS
    for d in dbs:
        dump_db(Path(d), outdir, limit=args.limit, include_views=not args.no_views, jsonl=not args.no_jsonl)

    log.info(f"Database dump completed. Output in: {outdir.resolve()}")
    print(f"\nDone. Output in: {outdir.resolve()}")

if __name__ == "__main__":
    main()
