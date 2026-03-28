from __future__ import annotations

from datetime import datetime


DEFAULT_DAG_ARGS = {
    "owner": "freightpulse",
    "depends_on_past": False,
    "retries": 0,
}


def airflow_available() -> bool:
    try:
        import airflow  # noqa: F401
        return True
    except ModuleNotFoundError:
        return False


START_DATE = datetime(2025, 1, 1)
