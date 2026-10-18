"""Pipeline task functions (used by the Airflow DAG and runnable from the command line).

Usage:  python steps.py extract|validate_raw|clean|load_staging|build_dimensions|build_fact|quality_checks|all
"""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

import pandas as pd

from nexusmart import cleaning, db


def _p(var, default):
    return Path(os.environ.get(var, default))


RAW = lambda: _p("NEXUS_RAW_DIR", "/opt/airflow/data/raw")
PROC = lambda: _p("NEXUS_PROCESSED_DIR", "/opt/airflow/data/processed")
REJ = lambda: _p("NEXUS_REJECTS_DIR", "/opt/airflow/data/rejects")
REP = lambda: _p("NEXUS_REPORTS_DIR", "/opt/airflow/reports")
SQL = lambda: _p("NEXUS_SQL_DIR", "/opt/airflow/sql")


def _run_id(run_id=None):
    return run_id or os.environ.get("NEXUS_RUN_ID") or time.strftime("manual_%Y%m%dT%H%M%S")


def _timed(task):
    def deco(fn):
        def wrapper(run_id=None, **kw):
            rid = _run_id(run_id)
            t0 = time.time()
            try:
                res = fn(rid, **kw) or {}
                db.log_task(rid, task, "success", res.get("read", 0), res.get("rejected", 0), res.get("loaded", 0),
                            f"{res.get('message', '')} ({time.time() - t0:.1f}s)")
                print(f"[{task}] OK read={res.get('read', 0)} rejected={res.get('rejected', 0)} loaded={res.get('loaded', 0)} {res.get('message', '')}")
                return res
            except Exception as exc:
                db.log_task(rid, task, "failed", message=repr(exc))
                raise
        wrapper.__name__ = fn.__name__
        return wrapper
    return deco


def _ensure_schema():
    c = db.connect()
    with c:
        db.run_sql_file(c, SQL() / "01_schema.sql")
    c.close()


# ------------------------------------------------------------------ extract / validate_raw
@_timed("extract")
def extract(run_id):
    _ensure_schema()
    info = cleaning.validate_raw(RAW())
    REP().mkdir(parents=True, exist_ok=True)
    pd.DataFrame([dict(file=k + ".csv", rows=v["rows"]) for k, v in info.items()]).to_csv(REP() / "raw_manifest.csv", index=False)
    total = sum(v["rows"] for v in info.values())
    return dict(read=total, message=f"{len(info)} raw files found")


@_timed("validate_raw")
def validate_raw(run_id):
    raw = cleaning.read_raw(RAW())
    rows, issues = [], 0
    for name, df in raw.items():
        key = df.columns[0]
        dup_rows = int(df.duplicated().sum())
        dup_keys = int(df[key].duplicated().sum())
        issues += dup_rows
        for c in df.columns:
            rows.append(dict(file=name, column=c, rows=len(df), blank_values=int((df[c] == "").sum()), distinct_values=int(df[c].nunique()),
                             exact_duplicate_rows=dup_rows if c == key else None, duplicate_keys=dup_keys if c == key else None))
    pd.DataFrame(rows).to_csv(REP() / "raw_profile.csv", index=False)
    total = sum(len(d) for d in raw.values())
    # hard failure only for empty files (structure was already verified in extract)
    empty = [k for k, d in raw.items() if len(d) == 0]
    if empty:
        raise ValueError(f"Empty raw files: {empty}")
    return dict(read=total, message=f"profile written; {issues} exact duplicate rows found in raw files")


# ------------------------------------------------------------------ clean
@_timed("clean")
def clean(run_id):
    s = cleaning.clean_all(RAW(), PROC(), REJ(), REP())
    raw_rows = sum(t["rows_raw"] for t in s["tables"].values())
    clean_rows = sum(t["rows_clean"] for t in s["tables"].values())
    rej = sum(s["rejects_by_reason"].values())
    if s["order_total_reconciliation_pct"] < 97.0:
        raise ValueError(f"order_total reconciliation too low: {s['order_total_reconciliation_pct']}%")
    return dict(read=raw_rows, rejected=rej, loaded=clean_rows,
                message=f"order_total reconciliation {s['order_total_reconciliation_pct']}%")


# ------------------------------------------------------------------ load_staging
STAGE_TABLES = ["locations", "sellers", "customers", "products", "orders", "order_items", "payments", "shipping", "returns",
                "customer_map", "product_map"]


@_timed("load_staging")
def load_staging(run_id):
    _ensure_schema()
    c = db.connect()
    total = 0
    try:
        with c:
            with c.cursor() as cur:
                cur.execute("TRUNCATE " + ", ".join(f"stg.{t}" for t in STAGE_TABLES))
                cur.execute("TRUNCATE ops.rejects, ops.dq_rules_log, ops.dq_table_counts")
            for t in STAGE_TABLES:
                f = PROC() / f"{t}.csv"
                db.copy_csv(c, f"stg.{t}", f)
                expected = sum(1 for _ in open(f, encoding="utf-8")) - 1 if t != "customers" else None
                loaded = db.scalar(c, f"SELECT count(*) FROM stg.{t}")
                if expected is not None and expected != loaded and t not in ("orders", "order_items", "payments", "returns"):
                    raise ValueError(f"{t}: CSV rows {expected} != staged rows {loaded}")
                total += loaded
            db.copy_csv(c, "ops.rejects", REJ() / "rejects.csv", ["table_name", "record_key", "reason_code", "action", "raw_detail"])
            db.copy_csv(c, "ops.dq_rules_log", REP() / "dq_rules_log.csv", ["table_name", "rule_id", "column_name", "issue", "action", "records_affected"])
            db.copy_csv(c, "ops.dq_table_counts", REP() / "dq_table_counts.csv", ["table_name", "rows_raw", "exact_duplicates_removed", "rows_rejected", "rows_clean"])
            rejected = db.scalar(c, "SELECT count(*) FROM ops.rejects")
    finally:
        c.close()
    return dict(read=total, rejected=rejected, loaded=total, message="staging reloaded (truncate + COPY)")


# ------------------------------------------------------------------ build_dimensions / build_fact
@_timed("build_dimensions")
def build_dimensions(run_id):
    c = db.connect()
    try:
        with c:
            db.run_sql_file(c, SQL() / "02_dimensions.sql")
            n = {t: db.scalar(c, f"SELECT count(*) FROM dw.{t}") for t in ["dim_date", "dim_customer", "dim_product", "dim_seller", "dim_location"]}
    finally:
        c.close()
    return dict(read=sum(n.values()), loaded=sum(n.values()), message=json.dumps(n))


@_timed("build_fact")
def build_fact(run_id):
    c = db.connect()
    try:
        with c:
            read = db.scalar(c, "SELECT count(*) FROM stg.order_items")
            db.run_sql_file(c, SQL() / "03_fact.sql")
            db.run_sql_file(c, SQL() / "04_analytics.sql")
            loaded = db.scalar(c, "SELECT count(*) FROM dw.fact_sales")
    finally:
        c.close()
    if read != loaded:
        raise ValueError(f"fact rows {loaded} != staged order lines {read}")
    return dict(read=read, loaded=loaded, message="FactSales rebuilt (grain = order line); RFM segments and seller scores refreshed")


# ------------------------------------------------------------------ quality_checks
@_timed("quality_checks")
def quality_checks(run_id):
    c = db.connect()
    results = []

    def chk(name, severity, expected, actual, passed):
        results.append((run_id, name, severity, str(expected), str(actual), bool(passed)))

    def one(sql):
        return db.scalar(c, sql)

    try:
        # 1. row-count conservation per table: raw = clean + exact duplicates removed + rejected/merged
        with c.cursor() as cur:
            cur.execute("SELECT table_name, rows_raw, exact_duplicates_removed, rows_rejected, rows_clean FROM ops.dq_table_counts")
            for t, raw, dup, rej, cln in cur.fetchall():
                chk(f"row_conservation_{t}", "critical", raw, cln + dup + rej, raw == cln + dup + rej)
        n_dup = one("SELECT COALESCE(SUM(exact_duplicates_removed),0) FROM ops.dq_table_counts")
        n_dup_logged = one("SELECT count(*) FROM ops.rejects WHERE reason_code='EXACT_DUPLICATE_ROW'")
        chk("duplicates_logged_in_rejects", "critical", n_dup, n_dup_logged, n_dup == n_dup_logged)

        # 2. staging vs fact
        stg_lines = one("SELECT count(*) FROM stg.order_items")
        fact_lines = one("SELECT count(*) FROM dw.fact_sales")
        chk("fact_rows_equal_staged_order_lines", "critical", stg_lines, fact_lines, stg_lines == fact_lines)
        chk("fact_grain_unique_order_item", "critical", 0, one("SELECT count(*) - count(DISTINCT order_item_id) FROM dw.fact_sales"),
            one("SELECT count(*) - count(DISTINCT order_item_id) FROM dw.fact_sales") == 0)
        so, fo = one("SELECT count(*) FROM stg.orders"), one("SELECT count(DISTINCT order_id) FROM dw.fact_sales")
        chk("orders_in_fact_equal_staged_orders", "critical", so, fo, so == fo)

        # 3. keys and unknown members
        nulls = one("SELECT count(*) FROM dw.fact_sales WHERE date_key IS NULL OR customer_key IS NULL OR product_key IS NULL OR seller_key IS NULL OR location_key IS NULL")
        chk("no_null_foreign_keys", "critical", 0, nulls, nulls == 0)
        for dim, key in (("dim_customer", "customer_key"), ("dim_product", "product_key"), ("dim_seller", "seller_key"),
                         ("dim_location", "location_key"), ("dim_date", "date_key")):
            has = one(f"SELECT count(*) FROM dw.{dim} WHERE {key} = -1")
            chk(f"unknown_member_{dim}", "critical", 1, has, has == 1)
        for col, label in (("customer_key", "customer"), ("product_key", "product"), ("seller_key", "seller"), ("location_key", "location")):
            pct = one(f"SELECT round(100.0 * count(*) FILTER (WHERE {col} = -1) / count(*), 3) FROM dw.fact_sales")
            chk(f"unknown_{label}_share_pct_max_2", "critical", "<= 2", pct, pct <= 2)
        links = one("""SELECT count(*) FROM pg_constraint k JOIN pg_class r ON r.oid = k.conrelid JOIN pg_namespace n ON n.oid = r.relnamespace
                       WHERE k.contype = 'f' AND n.nspname = 'dw' AND r.relname LIKE 'dim\\_%'""")
        chk("dimensions_not_linked_to_each_other", "critical", 0, links, links == 0)

        # 4. amounts reconcile with the cleaned staging data
        d = one("SELECT abs(COALESCE((SELECT sum(net_amount) FROM dw.fact_sales WHERE NOT is_cancelled),0) - COALESCE((SELECT sum(i.line_net) FROM stg.order_items i JOIN stg.orders o USING(order_id) WHERE o.order_status <> 'Cancelled'),0))")
        chk("nmv_fact_equals_staging", "critical", "<= 1 PKR", d, d <= 1)
        d = one("SELECT abs((SELECT sum(gross_amount) FROM dw.fact_sales) - (SELECT sum(gross) FROM stg.order_items))")
        chk("gross_fact_equals_staging", "critical", "<= 1 PKR", d, d <= 1)
        d = one("SELECT abs(COALESCE((SELECT sum(shipping_fee) FROM dw.fact_sales),0) - COALESCE((SELECT sum(s.shipping_fee) FROM stg.shipping s WHERE s.order_id IN (SELECT order_id FROM stg.order_items)),0))")
        chk("shipping_fee_allocated_without_double_count", "critical", "<= 1 PKR", d, d <= 1)
        d = one("""SELECT abs((SELECT COALESCE(sum(returned_amount),0) FROM dw.fact_sales) -
                   (SELECT COALESCE(sum(refund_amount),0) FROM stg.returns WHERE return_status <> 'Rejected'))""")
        chk("refunds_fact_equals_valid_returns", "critical", "<= 1 PKR", d, d <= 1)
        a = one("SELECT count(*) FROM dw.fact_sales WHERE is_returned")
        b = one("SELECT count(DISTINCT order_item_id) FROM stg.returns WHERE return_status <> 'Rejected'")
        chk("returned_lines_equal_distinct_valid_returns", "critical", b, a, a == b)
        rej_ret = one("SELECT count(*) FROM dw.fact_sales f JOIN stg.returns r USING(order_item_id) WHERE r.return_status = 'Rejected' AND NOT EXISTS (SELECT 1 FROM stg.returns v WHERE v.order_item_id = r.order_item_id AND v.return_status <> 'Rejected') AND f.is_returned")
        chk("rejected_returns_not_counted", "critical", 0, rej_ret, rej_ret == 0)

        # 5. value sanity
        bad = one("SELECT count(*) FROM dw.fact_sales WHERE quantity <= 0 OR unit_price <= 0 OR gross_amount < 0 OR net_amount < 0 OR cost_amount < 0 OR returned_amount < 0 OR discount_amount < 0 OR shipping_fee < 0")
        chk("no_negative_or_zero_amounts", "critical", 0, bad, bad == 0)
        mn, mx = one("SELECT min(d.full_date) FROM dw.fact_sales f JOIN dw.dim_date d USING(date_key)"), one("SELECT max(d.full_date) FROM dw.fact_sales f JOIN dw.dim_date d USING(date_key)")
        chk("order_dates_inside_valid_window", "critical", "2024-01-01..2025-06-30", f"{mn}..{mx}", str(mn) >= "2024-01-01" and str(mx) <= "2025-06-30")
        rec = one("SELECT round(100.0 * count(*) FILTER (WHERE abs(o.order_total - x.n) <= 2) / count(*), 2) FROM stg.orders o JOIN (SELECT order_id, sum(line_net) n FROM stg.order_items GROUP BY 1) x USING(order_id) WHERE o.order_total IS NOT NULL")
        chk("order_total_reconciliation_pct_min_98", "critical", ">= 98", rec, rec >= 98)

        # 5b. analytics layer
        buyers = one("SELECT count(DISTINCT customer_key) FROM dw.fact_sales WHERE NOT is_cancelled AND customer_key <> -1")
        seg = one("SELECT count(*) FROM dw.dim_customer WHERE customer_key <> -1 AND segment NOT IN ('No Purchases','Unassigned')")
        chk("rfm_segment_assigned_to_every_buyer", "critical", buyers, seg, buyers == seg)
        sc = one("SELECT count(*) FROM dw.dim_seller d WHERE seller_key <> -1 AND EXISTS (SELECT 1 FROM dw.fact_sales f WHERE f.seller_key = d.seller_key) AND (quality_score IS NULL OR quality_score NOT BETWEEN 0 AND 100)")
        chk("every_selling_seller_has_score_0_100", "critical", 0, sc, sc == 0)

        # 6. business-rate plausibility (warnings)
        canc = one("SELECT round(100.0 * count(DISTINCT order_id) FILTER (WHERE is_cancelled) / count(DISTINCT order_id), 2) FROM dw.fact_sales")
        chk("cancellation_rate_plausible_2_to_30", "warning", "2..30", canc, 2 <= canc <= 30)
        retr = one("SELECT round(100.0 * count(*) FILTER (WHERE is_returned AND is_delivered) / NULLIF(count(*) FILTER (WHERE is_delivered), 0), 2) FROM dw.fact_sales")
        chk("return_rate_plausible_0_to_40", "warning", "0..40", retr, 0 <= retr <= 40)
        with c:
            with c.cursor() as cur:
                cur.execute("DELETE FROM ops.quality_check_results WHERE run_id = %s", (run_id,))
                cur.executemany("INSERT INTO ops.quality_check_results (run_id, check_name, severity, expected, actual, passed) VALUES (%s,%s,%s,%s,%s,%s)", results)
        # run summary file
        with c.cursor() as cur:
            cur.execute("SELECT task, status, records_read, records_rejected, records_loaded, message FROM ops.pipeline_run_log WHERE run_id = %s ORDER BY id", (run_id,))
            tasks = [dict(zip(["task", "status", "read", "rejected", "loaded", "message"], r)) for r in cur.fetchall()]
        summary = dict(run_id=run_id, checks_total=len(results), checks_failed=[r[1] for r in results if not r[5]],
                       tasks=tasks, checks=[dict(zip(["run_id", "check", "severity", "expected", "actual", "passed"], r)) for r in results])
        REP().mkdir(parents=True, exist_ok=True)
        (REP() / "pipeline_run_summary.json").write_text(json.dumps(summary, indent=2, default=str))
    finally:
        c.close()
    crit = [r for r in results if r[2] == "critical" and not r[5]]
    warn = [r for r in results if r[2] == "warning" and not r[5]]
    if crit:
        raise ValueError("CRITICAL quality checks failed: " + "; ".join(f"{r[1]} expected {r[3]} got {r[4]}" for r in crit))
    return dict(read=len(results), loaded=len(results) - len(crit), message=f"{len(results)} checks passed, {len(warn)} warnings")


ALL = ["extract", "validate_raw", "clean", "load_staging", "build_dimensions", "build_fact", "quality_checks"]

if __name__ == "__main__":
    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    todo = ALL if which == "all" else [which]
    for name in todo:
        globals()[name]()
