from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from src.models.arima_model import ArimaForecaster
from src.models.base import ModelTrainingResult
from src.models.model_registry import MLflowModelRegistry
from src.models.prophet_model import ProphetForecaster
from src.models.xgboost_model import XGBoostForecaster
from src.storage.db_manager import DuckDBManager


@dataclass
class TrainingBundle:
    results: list[ModelTrainingResult]
    run_ids: list[str]


class ForecastingService:
    def __init__(self, db_manager: DuckDBManager | None = None) -> None:
        self.db_manager = db_manager or DuckDBManager()

    def _load_feature_frames(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        annual = self.db_manager.fetch_df(
            "SELECT * FROM feature_corridor_annual ORDER BY corridor_id, year"
        )
        enriched = self.db_manager.fetch_df(
            "SELECT * FROM feature_corridor_enriched ORDER BY corridor_id, year"
        )
        return annual, enriched

    def train_all_models(self, log_to_mlflow: bool = True) -> TrainingBundle:
        annual, enriched = self._load_feature_frames()
        annual_fred = self.db_manager.fetch_df("SELECT * FROM staged_fred_annual ORDER BY year")
        results: list[ModelTrainingResult] = []

        arima = ArimaForecaster()
        prophet = ProphetForecaster()
        for corridor_id in sorted(annual["corridor_id"].unique()):
            corridor_annual = annual[annual["corridor_id"] == corridor_id].copy()
            results.append(arima.train(corridor_annual))
            results.append(prophet.train(corridor_annual, annual_fred))

        xgb = XGBoostForecaster()
        results.extend(xgb.train(enriched, annual_fred))

        run_ids: list[str] = []
        if log_to_mlflow:
            registry = MLflowModelRegistry()
            for result in results:
                run_ids.append(registry.log_training_result(result))

        return TrainingBundle(results=results, run_ids=run_ids)
