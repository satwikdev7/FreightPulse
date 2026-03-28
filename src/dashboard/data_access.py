from __future__ import annotations

from pathlib import Path

import pandas as pd
from mlflow.tracking import MlflowClient

from src.storage.db_manager import DuckDBManager
from src.utils.config import CONFIG
from src.utils import paths


class DashboardDataAccess:
    """Load dashboard-facing data from DuckDB, CSV artifacts, and MLflow."""

    def __init__(self, db_manager: DuckDBManager | None = None) -> None:
        self.db_manager = db_manager or DuckDBManager(read_only=True)

    def load_corridor_annual(self) -> pd.DataFrame:
        return self.db_manager.fetch_df(
            "SELECT * FROM feature_corridor_annual ORDER BY corridor_id, year"
        )

    def load_corridor_enriched(self) -> pd.DataFrame:
        return self.db_manager.fetch_df(
            "SELECT * FROM feature_corridor_enriched ORDER BY corridor_id, year"
        )

    def load_bts_forecast(self) -> pd.DataFrame:
        return self.db_manager.fetch_df(
            "SELECT * FROM feature_bts_forecast ORDER BY corridor_id, year"
        )

    def load_model_comparison(self) -> pd.DataFrame:
        path = paths.PROCESSED_EVALUATION_DIR / "model_comparison.csv"
        return pd.read_csv(path) if path.exists() else pd.DataFrame()

    def load_holdout_scores(self) -> pd.DataFrame:
        path = paths.PROCESSED_EVALUATION_DIR / "holdout_scores.csv"
        return pd.read_csv(path) if path.exists() else pd.DataFrame()

    def load_bts_scores(self) -> pd.DataFrame:
        path = paths.PROCESSED_EVALUATION_DIR / "bts_benchmark_scores.csv"
        return pd.read_csv(path) if path.exists() else pd.DataFrame()

    def load_forecasts(self) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []
        for csv_path in sorted(paths.PROCESSED_FORECASTS_DIR.glob("*_corridor_*_forecasts.csv")):
            frames.append(pd.read_csv(csv_path))
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def load_feature_importance(self) -> pd.DataFrame:
        path = paths.PROCESSED_FORECASTS_DIR / "xgboost_feature_importance.csv"
        return pd.read_csv(path) if path.exists() else pd.DataFrame()

    def load_mlflow_runs(self) -> pd.DataFrame:
        client = MlflowClient(tracking_uri=CONFIG.mlflow_tracking_uri)
        experiments = [
            experiment
            for experiment in client.search_experiments()
            if experiment.name == CONFIG.mlflow_experiment_name
        ]
        if not experiments:
            return pd.DataFrame()

        runs = client.search_runs([experiments[0].experiment_id])
        rows: list[dict[str, object]] = []
        for run in runs:
            rows.append(
                {
                    "run_id": run.info.run_id,
                    "status": run.info.status,
                    "corridor_id": run.data.params.get("corridor_id"),
                    "corridor_name": run.data.params.get("corridor_name"),
                    "model_name": run.data.params.get("model_name"),
                    "training_strategy": run.data.params.get("training_strategy"),
                    "mape": run.data.metrics.get("mape"),
                    "rmse": run.data.metrics.get("rmse"),
                    "mae": run.data.metrics.get("mae"),
                }
            )
        return pd.DataFrame(rows)
