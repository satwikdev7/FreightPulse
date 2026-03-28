from __future__ import annotations

import json
from pathlib import Path

import mlflow

from src.models.base import ModelTrainingResult
from src.utils.config import CONFIG
from src.utils import paths
from src.utils.paths import ensure_project_directories


class MLflowModelRegistry:
    """Small helper around MLflow experiment logging for model runs."""

    def __init__(self, tracking_uri: str | None = None, experiment_name: str | None = None) -> None:
        ensure_project_directories()
        self.tracking_uri = tracking_uri or CONFIG.mlflow_tracking_uri
        self.experiment_name = experiment_name or CONFIG.mlflow_experiment_name
        mlflow.set_tracking_uri(self.tracking_uri)
        mlflow.set_experiment(self.experiment_name)

    def log_training_result(self, result: ModelTrainingResult) -> str:
        run_name = (
            f"{result.model_name}_corridor_{result.corridor_id}_{result.training_strategy}"
        )
        with mlflow.start_run(run_name=run_name) as run:
            mlflow.set_tags(
                {
                    "corridor_id": result.corridor_id,
                    "corridor_name": result.corridor_name,
                    "model_name": result.model_name,
                    "training_strategy": result.training_strategy,
                }
            )
            mlflow.log_params(
                {
                    "corridor_id": result.corridor_id,
                    "corridor_name": result.corridor_name,
                    "model_name": result.model_name,
                    "training_strategy": result.training_strategy,
                    **result.params,
                }
            )
            mlflow.log_metrics(result.metrics)

            for artifact_name, artifact_path in result.artifacts.items():
                path = Path(artifact_path)
                if path.exists():
                    mlflow.log_artifact(str(path), artifact_path=f"artifacts/{artifact_name}")

            forecast_path = paths.PROCESSED_FORECASTS_DIR / (
                f"{result.model_name.lower()}_corridor_{result.corridor_id}_forecasts.csv"
            )
            forecast_path.parent.mkdir(parents=True, exist_ok=True)
            result.forecasts.to_csv(forecast_path, index=False)
            mlflow.log_artifact(str(forecast_path), artifact_path="forecasts")

            metrics_json_path = paths.PROCESSED_EVALUATION_DIR / (
                f"{result.model_name.lower()}_corridor_{result.corridor_id}_metrics.json"
            )
            metrics_json_path.parent.mkdir(parents=True, exist_ok=True)
            metrics_json_path.write_text(json.dumps(result.metrics, indent=2))
            mlflow.log_artifact(str(metrics_json_path), artifact_path="metrics")

            return run.info.run_id
