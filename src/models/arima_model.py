from __future__ import annotations

import pickle
from pathlib import Path

import numpy as np
import pandas as pd
from statsmodels.tsa.arima.model import ARIMA

from src.evaluation.metrics import compute_regression_metrics
from src.models.base import ForecastRecord, ModelTrainingResult
from src.models.common import (
    ARIMA_PROPHET_HORIZON,
    HOLDOUT_YEAR,
    TRAIN_END_YEAR,
    build_forecast_frame,
    ensure_model_output_dirs,
)


class ArimaForecaster:
    model_name = "ARIMA"
    training_strategy = "leave_last_year_out"

    def __init__(self, order_grid: list[tuple[int, int, int]] | None = None) -> None:
        self.order_grid = order_grid or [(0, 1, 0), (1, 1, 0), (1, 1, 1), (2, 1, 0)]

    def _fit_best_model(self, series: pd.Series) -> tuple[ARIMA, tuple[int, int, int], float]:
        best_model = None
        best_order = None
        best_aic = float("inf")
        for order in self.order_grid:
            try:
                fitted = ARIMA(series, order=order).fit()
                if fitted.aic < best_aic:
                    best_model = fitted
                    best_order = order
                    best_aic = float(fitted.aic)
            except Exception:
                continue

        if best_model is None or best_order is None:
            fallback = ARIMA(series, order=(1, 1, 0)).fit()
            return fallback, (1, 1, 0), float(fallback.aic)
        return best_model, best_order, best_aic

    def train(self, corridor_frame: pd.DataFrame) -> ModelTrainingResult:
        corridor_frame = corridor_frame.sort_values("year").reset_index(drop=True)
        train_frame = corridor_frame[corridor_frame["year"] <= TRAIN_END_YEAR].copy()
        full_frame = corridor_frame.copy()

        train_series = pd.Series(
            train_frame["total_tons"].to_numpy(),
            index=pd.to_datetime(train_frame["year"].astype(str) + "-01-01"),
        )
        holdout_actual = float(full_frame.loc[full_frame["year"] == HOLDOUT_YEAR, "total_tons"].iloc[0])

        holdout_model, selected_order, best_aic = self._fit_best_model(train_series)
        holdout_forecast = holdout_model.get_forecast(steps=1)
        holdout_prediction = float(holdout_forecast.predicted_mean.iloc[0])
        holdout_ci = holdout_forecast.conf_int(alpha=0.2).iloc[0]

        full_series = pd.Series(
            full_frame["total_tons"].to_numpy(),
            index=pd.to_datetime(full_frame["year"].astype(str) + "-01-01"),
        )
        full_model, _, _ = self._fit_best_model(full_series)
        future_forecast = full_model.get_forecast(steps=len(ARIMA_PROPHET_HORIZON))
        future_mean = future_forecast.predicted_mean.tolist()
        future_ci = future_forecast.conf_int(alpha=0.2)

        records = [
            ForecastRecord(
                corridor_id=int(corridor_frame["corridor_id"].iloc[0]),
                corridor_name=str(corridor_frame["corridor_name"].iloc[0]),
                model_name=self.model_name,
                training_strategy=self.training_strategy,
                forecast_type="holdout",
                year=HOLDOUT_YEAR,
                prediction=holdout_prediction,
                lower_bound=float(holdout_ci.iloc[0]),
                upper_bound=float(holdout_ci.iloc[1]),
            )
        ]
        for idx, year in enumerate(ARIMA_PROPHET_HORIZON):
            records.append(
                ForecastRecord(
                    corridor_id=int(corridor_frame["corridor_id"].iloc[0]),
                    corridor_name=str(corridor_frame["corridor_name"].iloc[0]),
                    model_name=self.model_name,
                    training_strategy=self.training_strategy,
                    forecast_type="future",
                    year=year,
                    prediction=float(future_mean[idx]),
                    lower_bound=float(future_ci.iloc[idx, 0]),
                    upper_bound=float(future_ci.iloc[idx, 1]),
                )
            )

        metrics = compute_regression_metrics([holdout_actual], [holdout_prediction])
        params = {
            "order": str(selected_order),
            "training_rows": int(len(train_frame)),
            "full_rows": int(len(full_frame)),
            "aic": best_aic,
        }

        models_dir, _ = ensure_model_output_dirs()
        model_path = models_dir / f"arima_corridor_{int(corridor_frame['corridor_id'].iloc[0])}.pkl"
        with model_path.open("wb") as buffer:
            pickle.dump(full_model, buffer)

        result = ModelTrainingResult(
            corridor_id=int(corridor_frame["corridor_id"].iloc[0]),
            corridor_name=str(corridor_frame["corridor_name"].iloc[0]),
            model_name=self.model_name,
            training_strategy=self.training_strategy,
            params=params,
            metrics=metrics,
            forecasts=build_forecast_frame(records),
            model_object=full_model,
            artifacts={"model_pickle": model_path},
        )
        result.validate_forecast_schema()
        return result
