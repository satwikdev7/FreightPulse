from __future__ import annotations

from src.storage.feature_layer import FeatureLayerBuilder
from src.storage.staged_layer import StagedLayerBuilder


def stage_domestic():
    return StagedLayerBuilder().build_staged_faf_domestic()


def stage_historical():
    return StagedLayerBuilder().build_staged_faf_historical_domestic()


def stage_fred():
    return StagedLayerBuilder().build_staged_fred_annual()


def build_zone_state_map():
    return StagedLayerBuilder().build_staged_zone_to_state()


def build_corridor_annual():
    return FeatureLayerBuilder().build_feature_corridor_annual()


def build_features():
    return FeatureLayerBuilder().build_feature_corridor_enriched()


def build_historical():
    return FeatureLayerBuilder().build_feature_corridor_historical()


def build_bts_forecast():
    return FeatureLayerBuilder().build_feature_bts_forecast()


try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator

    from dags.common import DEFAULT_DAG_ARGS, START_DATE

    with DAG(
        dag_id="transform",
        default_args=DEFAULT_DAG_ARGS,
        start_date=START_DATE,
        schedule=None,
        catchup=False,
        tags=["freightpulse", "transform"],
    ) as dag:
        stage_domestic_task = PythonOperator(task_id="stage_domestic", python_callable=stage_domestic)
        stage_historical_task = PythonOperator(task_id="stage_historical", python_callable=stage_historical)
        stage_fred_task = PythonOperator(task_id="stage_fred", python_callable=stage_fred)
        build_zone_state_map_task = PythonOperator(task_id="build_zone_state_map", python_callable=build_zone_state_map)
        build_corridor_annual_task = PythonOperator(task_id="build_corridor_annual", python_callable=build_corridor_annual)
        build_features_task = PythonOperator(task_id="build_features", python_callable=build_features)
        build_historical_task = PythonOperator(task_id="build_historical", python_callable=build_historical)
        build_bts_forecast_task = PythonOperator(task_id="build_bts_forecast", python_callable=build_bts_forecast)

        [stage_domestic_task, stage_historical_task, stage_fred_task, build_zone_state_map_task] >> build_corridor_annual_task
        build_corridor_annual_task >> build_features_task
        stage_historical_task >> build_historical_task
        stage_domestic_task >> build_bts_forecast_task
except Exception:
    dag = None
