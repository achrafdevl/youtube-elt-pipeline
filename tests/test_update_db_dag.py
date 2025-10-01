# tests/test_update_db_dag.py
import importlib
import os
import sys
from contextlib import contextmanager

import pytest
from airflow.models import DagBag

@contextmanager
def temp_env(env: dict):
    old = {}
    try:
        for k, v in env.items():
            old[k] = os.environ.get(k)
            if v is None and k in os.environ:
                del os.environ[k]
            elif v is not None:
                os.environ[k] = v
        yield
    finally:
        for k, v in old.items():
            if v is None and k in os.environ:
                del os.environ[k]
            elif v is not None:
                os.environ[k] = v

def _reload_update_db_dag():
    # Ensure project root is on path so `dags` and `src` resolve during test runs
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)
    if "dags.update_db_dag" in sys.modules:
        del sys.modules["dags.update_db_dag"]
    return importlib.import_module("dags.update_db_dag")

def _get_dag_from_bag():
    bag = DagBag(include_examples=False)  # validate importability
    assert not bag.import_errors, f"Dag import errors: {bag.import_errors}"
    dag = bag.get_dag("update_db")
    assert dag is not None, "DAG 'update_db' not found"
    return dag

def test_dag_imports_and_has_basic_attrs():
    # Default env: DB enabled, Soda enabled by default per module, but safe to import
    _reload_update_db_dag()
    dag = _get_dag_from_bag()
    assert dag.default_args.get("owner") == "airflow"
    assert dag.schedule == "30 2 * * *"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    # Must have at least the two core tasks in any mode
    task_ids = {t.task_id for t in dag.tasks}
    assert "load_staging" in task_ids
    assert "transform_core" in task_ids

def test_tasks_when_db_enabled(monkeypatch):
    # Enable DB, disable Soda to simplify
    with temp_env({"DISABLE_DB": "0", "ENABLE_SODA": "0"}):
        mod = _reload_update_db_dag()
        dag = _get_dag_from_bag()
        task_ids = [t.task_id for t in dag.tasks]
        assert set(task_ids) == {"load_staging", "transform_core"}
        # Check python_callables are lambdas (db-enabled path uses lambda wrappers)
        load = dag.get_task("load_staging")
        transform = dag.get_task("transform_core")
        assert load.python_callable.__name__ == "<lambda>"
        assert transform.python_callable.__name__ == "<lambda>"
        # Dependency: load_staging >> transform_core
        upstream = {t.task_id for t in transform.upstream_list}
        assert upstream == {"load_staging"}

def test_tasks_when_db_disabled(monkeypatch):
    with temp_env({"DISABLE_DB": "1", "ENABLE_SODA": "0"}):
        mod = _reload_update_db_dag()
        dag = _get_dag_from_bag()
        task_ids = {t.task_id for t in dag.tasks}
        assert task_ids == {"load_staging", "transform_core"}
        load = dag.get_task("load_staging")
        transform = dag.get_task("transform_core")
        # In disabled mode, module defines named no-op callables
        assert load.python_callable.__name__ == "_noop_load"
        assert transform.python_callable.__name__ == "_noop_transform"
        # Still keep ordering (even if not strictly required)
        assert {t.task_id for t in transform.upstream_list} == {"load_staging"}

def test_soda_task_toggle():
    # Soda enabled
    with temp_env({"DISABLE_DB": "0", "ENABLE_SODA": "1"}):
        _reload_update_db_dag()
        dag = _get_dag_from_bag()
        task_ids = {t.task_id for t in dag.tasks}
        assert "soda_scan" in task_ids
        soda = dag.get_task("soda_scan")
        # Ensure it is downstream of transform_core
        assert {t.task_id for t in soda.upstream_list} == {"transform_core"}

    # Soda disabled
    with temp_env({"DISABLE_DB": "0", "ENABLE_SODA": "0"}):
        _reload_update_db_dag()
        dag = _get_dag_from_bag()
        task_ids = {t.task_id for t in dag.tasks}
        assert "soda_scan" not in task_ids