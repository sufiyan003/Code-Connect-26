"""Database helpers. Credentials come only from the NEXUS_DB_URL environment variable."""
from __future__ import annotations

import os
from pathlib import Path

import psycopg2
from sqlalchemy.engine import make_url


def connect():
    u = make_url(os.environ["NEXUS_DB_URL"])
    return psycopg2.connect(host=u.host, port=u.port or 5432, dbname=u.database, user=u.username, password=u.password)


def run_sql_file(conn, path):
    sql = Path(path).read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(sql)


def copy_csv(conn, table, csv_path, columns=None):
    cols = f" ({', '.join(columns)})" if columns else ""
    with conn.cursor() as cur, open(csv_path, "r", encoding="utf-8", newline="") as fh:
        cur.copy_expert(f"COPY {table}{cols} FROM STDIN WITH (FORMAT csv, HEADER true)", fh)


def scalar(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()[0]


def log_task(run_id, task, status, read=0, rejected=0, loaded=0, message=""):
    """Write a pipeline log row using its own short connection so it survives task rollbacks."""
    try:
        c = connect()
        with c, c.cursor() as cur:
            cur.execute("INSERT INTO ops.pipeline_run_log (run_id, task, status, records_read, records_rejected, records_loaded, message) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s)", (run_id, task, status, read, rejected, loaded, message[:2000]))
        c.close()
    except Exception as exc:  # logging must never break the pipeline
        print(f"[warn] could not write pipeline log: {exc}")
