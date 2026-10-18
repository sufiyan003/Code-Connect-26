"""NexusMart end-to-end pipeline.

extract -> validate_raw -> clean -> load_staging -> build_dimensions -> build_fact -> quality_checks

* Paths and DB credentials come from environment variables (see docker-compose.yml).
* Idempotent: staging is truncated and reloaded, dimensions are upserted on business keys,
  FactSales is rebuilt, so repeated runs never create duplicate records.
* Each task logs records read / rejected / loaded to ops.pipeline_run_log.
* quality_checks raises (fails the DAG run) when a critical check fails.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from nexusmart import steps


def _extract(run_id):
    steps.extract(run_id=run_id)


def _validate_raw(run_id):
    steps.validate_raw(run_id=run_id)


def _clean(run_id):
    steps.clean(run_id=run_id)


def _load_staging(run_id):
    steps.load_staging(run_id=run_id)


def _build_dimensions(run_id):
    steps.build_dimensions(run_id=run_id)


def _build_fact(run_id):
    steps.build_fact(run_id=run_id)


def _quality_checks(run_id):
    steps.quality_checks(run_id=run_id)


default_args = {
    "owner": "nexusmart",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="nexusmart_pipeline",
    description="Raw CSV -> cleaned data -> PostgreSQL star schema -> quality checks",
    start_date=datetime(2026, 10, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["nexusmart", "datathon"],
) as dag:
    kw = {"run_id": "{{ run_id }}"}
    t_extract = PythonOperator(task_id="extract", python_callable=_extract, op_kwargs=kw)
    t_validate = PythonOperator(task_id="validate_raw", python_callable=_validate_raw, op_kwargs=kw)
    t_clean = PythonOperator(task_id="clean", python_callable=_clean, op_kwargs=kw)
    t_stage = PythonOperator(task_id="load_staging", python_callable=_load_staging, op_kwargs=kw)
    t_dims = PythonOperator(task_id="build_dimensions", python_callable=_build_dimensions, op_kwargs=kw)
    t_fact = PythonOperator(task_id="build_fact", python_callable=_build_fact, op_kwargs=kw)
    t_qc = PythonOperator(task_id="quality_checks", python_callable=_quality_checks, op_kwargs=kw)

    t_extract >> t_validate >> t_clean >> t_stage >> t_dims >> t_fact >> t_qc
