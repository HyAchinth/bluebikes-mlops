import importlib.util
import pathlib
import pytest

# Skip tests if Airflow is not installed in this environment (CI may skip)
airflow = pytest.importorskip("airflow")

ROOT = pathlib.Path(__file__).resolve().parents[1]
DAGS = {
    "dag_ingest_gbfs": {
        "path": ROOT / "airflow" / "dags" / "dag_ingest_gbfs.py",
        "tasks": {"resolve_feeds", "snapshot_gbfs"},
    },
    "dag_ingest_weather": {
        "path": ROOT / "airflow" / "dags" / "dag_ingest_weather.py",
        "tasks": {"pull_weather"},
    },
    "dag_build_features": {
        "path": ROOT / "airflow" / "dags" / "dag_build_features.py",
        "tasks": {"bronze_clean", "add_calendar", "write_feature_store"},
    },
}


def _load_module(path: pathlib.Path):
    spec = importlib.util.spec_from_file_location(path.stem, path)
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(mod)  # type: ignore
    return mod


@pytest.mark.parametrize("dag_id", list(DAGS.keys()))
def test_dag_import_and_tasks(dag_id):
    info = DAGS[dag_id]
    mod = _load_module(info["path"])
    assert hasattr(mod, "dag"), f"{dag_id} must define a top-level 'dag'"
    dag = getattr(mod, "dag")
    assert dag.dag_id == dag_id
    task_ids = {t.task_id for t in dag.tasks}
    assert info["tasks"].issubset(task_ids)
