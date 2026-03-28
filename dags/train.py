from __future__ import annotations

from src.models.forecasting import ForecastingService


def train_all_models():
    return ForecastingService().train_all_models(log_to_mlflow=True)


try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator

    from dags.common import DEFAULT_DAG_ARGS, START_DATE

    with DAG(
        dag_id="train",
        default_args=DEFAULT_DAG_ARGS,
        start_date=START_DATE,
        schedule=None,
        catchup=False,
        tags=["freightpulse", "training"],
    ) as dag:
        train_models_task = PythonOperator(task_id="train_all_models", python_callable=train_all_models)
except Exception:
    dag = None
