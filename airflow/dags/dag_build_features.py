from __future__ import annotations
import os
import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

TZ = os.getenv("TZ", "America/New_York")
WORKSPACE = "/workspace"


def _bronze_clean(execution_date=None, **context):
    from scripts.build_features import bronze_clean
    bronze_clean(root=os.path.join(WORKSPACE, "data"), tz_name=TZ, ds=str(execution_date.date()))


def _add_calendar(execution_date=None, **context):
    from scripts.build_features import add_calendar
    add_calendar(root=os.path.join(WORKSPACE, "data"), tz_name=TZ, ds=str(execution_date.date()))


def _write_feature_store(execution_date=None, **context):
    from scripts.build_features import write_feature_store
    write_feature_store(root=os.path.join(WORKSPACE, "data"), tz_name=TZ, ds=str(execution_date.date()))


with DAG(
    dag_id="dag_build_features",
    description="Daily transform raw -> bronze -> feature_store (with calendar flags)",
    start_date=pendulum.now(TZ).subtract(days=1),
    schedule_interval="@daily",
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    max_active_runs=1,
    tags=["features"],
) as dag:

    bronze_clean_task = PythonOperator(
        task_id="bronze_clean",
        python_callable=_bronze_clean,
        provide_context=True,
    )

    add_calendar_task = PythonOperator(
        task_id="add_calendar",
        python_callable=_add_calendar,
        provide_context=True,
    )

    write_feature_store_task = PythonOperator(
        task_id="write_feature_store",
        python_callable=_write_feature_store,
        provide_context=True,
    )

    bronze_clean_task >> add_calendar_task >> write_feature_store_task


