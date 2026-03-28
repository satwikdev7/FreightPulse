from __future__ import annotations

from src.storage.raw_layer import RawLayerLoader


def download_check() -> bool:
    return True


def load_hilo_raw():
    return RawLayerLoader().load_hilo()


def load_historical_raw():
    return RawLayerLoader().load_historical()


def load_metadata():
    return RawLayerLoader().load_metadata()


try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator

    from dags.common import DEFAULT_DAG_ARGS, START_DATE

    with DAG(
        dag_id="ingest_faf",
        default_args=DEFAULT_DAG_ARGS,
        start_date=START_DATE,
        schedule=None,
        catchup=False,
        tags=["freightpulse", "ingestion"],
    ) as dag:
        download_check_task = PythonOperator(task_id="download_check", python_callable=download_check)
        load_hilo_task = PythonOperator(task_id="load_hilo_raw", python_callable=load_hilo_raw)
        load_historical_task = PythonOperator(
            task_id="load_historical_raw",
            python_callable=load_historical_raw,
        )
        load_metadata_task = PythonOperator(task_id="load_metadata", python_callable=load_metadata)

        download_check_task >> [load_hilo_task, load_historical_task, load_metadata_task]
except Exception:
    dag = None
