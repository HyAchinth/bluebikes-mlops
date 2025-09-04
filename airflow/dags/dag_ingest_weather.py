from __future__ import annotations
import os
import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

TZ = os.getenv("TZ", "America/New_York")
WORKSPACE = "/workspace"


def _pull_weather(**context):
    from scripts.pull_weather import main as weather_main

    weather_main(
        out_root=os.path.join(WORKSPACE, "data"),
        tz_name=TZ,
    )


with DAG(
    dag_id="dag_ingest_weather",
    description="Fetch hourly OpenWeather One Call and store hourly parquet",
    start_date=pendulum.now(TZ).subtract(days=1),
    schedule_interval="@hourly",
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    max_active_runs=1,
    tags=["ingest", "weather"],
) as dag:

    pull_weather = PythonOperator(
        task_id="pull_weather",
        python_callable=_pull_weather,
        provide_context=True,
    )
