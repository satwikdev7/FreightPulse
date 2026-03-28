from __future__ import annotations

from src.storage.raw_layer import RawLayerLoader


def pull_series(series_ids: list[str] | None = None):
    return RawLayerLoader().load_fred(series_ids=series_ids)


try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator

    from dags.common import DEFAULT_DAG_ARGS, START_DATE

    with DAG(
        dag_id="ingest_fred",
        default_args=DEFAULT_DAG_ARGS,
        start_date=START_DATE,
        schedule="@monthly",
        catchup=False,
        tags=["freightpulse", "ingestion"],
    ) as dag:
        pull_gdp = PythonOperator(task_id="pull_gdp", python_callable=pull_series, op_kwargs={"series_ids": ["GDPC1"]})
        pull_diesel = PythonOperator(task_id="pull_diesel", python_callable=pull_series, op_kwargs={"series_ids": ["GASDESW"]})
        pull_ipi = PythonOperator(task_id="pull_ipi", python_callable=pull_series, op_kwargs={"series_ids": ["INDPRO"]})
        pull_unemployment = PythonOperator(task_id="pull_unemployment", python_callable=pull_series, op_kwargs={"series_ids": ["UNRATE"]})
        pull_ppi = PythonOperator(task_id="pull_ppi", python_callable=pull_series, op_kwargs={"series_ids": ["PCU484484"]})
        pull_vmt = PythonOperator(task_id="pull_vmt", python_callable=pull_series, op_kwargs={"series_ids": ["TRFVOLUSM227NFWA"]})
except Exception:
    dag = None
