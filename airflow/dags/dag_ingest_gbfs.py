from __future__ import annotations
import os
import json
import requests
import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

TZ = os.getenv("TZ", "America/New_York")
GBFS_ROOT = os.getenv("GBFS_ROOT", "https://gbfs.bluebikes.com/gbfs/gbfs.json")
WORKSPACE = "/workspace"


def _resolve_feeds(**context):
    """Discover station_status and station_information URLs from the GBFS root."""
    resp = requests.get(GBFS_ROOT, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    # Prefer 'en' if present; else pick the first available language
    feeds_by_lang = data.get("data", {})
    lang = "en" if "en" in feeds_by_lang else next(iter(feeds_by_lang), None)
    if not lang:
        raise RuntimeError("No GBFS feeds found")
    feeds = feeds_by_lang[lang]["feeds"]
    feed_map = {f["name"]: f["url"] for f in feeds}
    urls = {
        "station_status": feed_map.get("station_status"),
        "station_information": feed_map.get("station_information"),
    }
    if not urls["station_status"] or not urls["station_information"]:
        raise RuntimeError("Required GBFS feeds not found in root")
    # push to XCom
    return urls


def _snapshot_gbfs(**context):
    """Call scripts.pull_gbfs_snapshot:main with resolved URLs and write outputs."""
    from scripts.pull_gbfs_snapshot import main as gbfs_main

    ti = context["ti"]
    urls = ti.xcom_pull(task_ids="resolve_feeds")
    if not urls:
        raise RuntimeError("resolve_feeds produced no URLs")

    gbfs_main(
        station_status_url=urls["station_status"],
        station_info_url=urls["station_information"],
        out_root=os.path.join(WORKSPACE, "data"),
        tz_name=TZ,
    )


with DAG(
    dag_id="dag_ingest_gbfs",
    description="Fetch GBFS snapshots every 5 minutes and append slim parquet",
    start_date=pendulum.now(TZ).subtract(days=1),
    schedule_interval="*/5 * * * *",
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    max_active_runs=1,
    tags=["ingest", "gbfs"],
) as dag:

    resolve_feeds = PythonOperator(
        task_id="resolve_feeds",
        python_callable=_resolve_feeds,
    )

    snapshot_gbfs = PythonOperator(
        task_id="snapshot_gbfs",
        python_callable=_snapshot_gbfs,
        provide_context=True,
    )

    resolve_feeds >> snapshot_gbfs
