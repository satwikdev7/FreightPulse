from __future__ import annotations

from src.evaluation.model_comparison import run_full_evaluation


def score_models():
    return run_full_evaluation(retrain_if_missing=False)


try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator

    from dags.common import DEFAULT_DAG_ARGS, START_DATE

    with DAG(
        dag_id="evaluate",
        default_args=DEFAULT_DAG_ARGS,
        start_date=START_DATE,
        schedule=None,
        catchup=False,
        tags=["freightpulse", "evaluation"],
    ) as dag:
        score_holdout = PythonOperator(task_id="score_holdout", python_callable=score_models)
except Exception:
    dag = None
