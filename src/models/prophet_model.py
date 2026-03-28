from __future__ import annotations

import pickle

import pandas as pd

from src.evaluation.metrics import compute_regression_metrics
from src.models.base import ForecastRecord, ModelTrainingResult
from src.models.common import (
    ARIMA_PROPHET_HORIZON,
    HOLDOUT_YEAR,
    TRAIN_END_YEAR,
    build_forecast_frame,
    build_future_regressors,
    ensure_model_output_dirs,
)


PROPHET_REGRESSORS = ["gdp", "diesel_price", "industrial_production", "unemployment_rate"]


class ProphetForecaster:
    model_name = "Prophet"
    training_strategy = "leave_last_year_out"

    @staticmethod
    def _prepare_prophet_frame(frame: pd.DataFrame) -> pd.DataFrame:
        prophet_frame = frame.copy()
        prophet_frame["ds"] = pd.to_datetime(prophet_frame["year"].astype(str) + "-01-01")
        prophet_frame["y"] = prophet_frame["total_tons"]
        return prophet_frame

    def _build_model(self):
        from prophet import Prophet

        model = Prophet(
            yearly_seasonality=False,
            weekly_seasonality=False,
            daily_seasonality=False,
            interval_width=0.8,
            changepoint_prior_scale=0.05,
        )
        for regressor in PROPHET_REGRESSORS:
            model.add_regressor(regressor)
        return model

    def train(self, corridor_frame: pd.DataFrame, annual_fred_frame: pd.DataFrame) -> ModelTrainingResult:
        corridor_frame = corridor_frame.sort_values("year").reset_index(drop=True)
        train_frame = corridor_frame[corridor_frame["year"] <= TRAIN_END_YEAR].copy()
        holdout_frame = corridor_frame[corridor_frame["year"] == HOLDOUT_YEAR].copy()

        train_prophet = self._prepare_prophet_frame(train_frame)
        holdout_prophet = self._prepare_prophet_frame(holdout_frame)

        holdout_model = self._build_model()
        holdout_model.fit(train_prophet[["ds", "y", *PROPHET_REGRESSORS]])
        holdout_prediction_frame = holdout_model.predict(holdout_prophet[["ds", *PROPHET_REGRESSORS]])
        holdout_prediction = float(holdout_prediction_frame["yhat"].iloc[0])
        holdout_actual = float(holdout_frame["total_tons"].iloc[0])

        full_prophet = self._prepare_prophet_frame(corridor_frame)
        future_regressors = build_future_regressors(corridor_frame, annual_fred_frame, ARIMA_PROPHET_HORIZON)
        future_prophet = future_regressors.copy()
        future_prophet["ds"] = pd.to_datetime(future_prophet["year"].astype(str) + "-01-01")

        future_model = self._build_model()
        future_model.fit(full_prophet[["ds", "y", *PROPHET_REGRESSORS]])
        future_prediction_frame = future_model.predict(future_prophet[["ds", *PROPHET_REGRESSORS]])

        records = [
            ForecastRecord(
                corridor_id=int(corridor_frame["corridor_id"].iloc[0]),
                corridor_name=str(corridor_frame["corridor_name"].iloc[0]),
                model_name=self.model_name,
                training_strategy=self.training_strategy,
                forecast_type="holdout",
                year=HOLDOUT_YEAR,
                prediction=holdout_prediction,
                lower_bound=float(holdout_prediction_frame["yhat_lower"].iloc[0]),
                upper_bound=float(holdout_prediction_frame["yhat_upper"].iloc[0]),
            )
        ]
        for _, row in future_prediction_frame.iterrows():
            records.append(
                ForecastRecord(
                    corridor_id=int(corridor_frame["corridor_id"].iloc[0]),
                    corridor_name=str(corridor_frame["corridor_name"].iloc[0]),
                    model_name=self.model_name,
                    training_strategy=self.training_strategy,
                    forecast_type="future",
                    year=int(row["ds"].year),
                    prediction=float(row["yhat"]),
                    lower_bound=float(row["yhat_lower"]),
                    upper_bound=float(row["yhat_upper"]),
                )
            )

        metrics = compute_regression_metrics([holdout_actual], [holdout_prediction])
        params = {
            "regressors": ",".join(PROPHET_REGRESSORS),
            "training_rows": int(len(train_frame)),
            "full_rows": int(len(corridor_frame)),
        }

        models_dir, _ = ensure_model_output_dirs()
        model_path = models_dir / f"prophet_corridor_{int(corridor_frame['corridor_id'].iloc[0])}.pkl"
        with model_path.open("wb") as buffer:
            pickle.dump(future_model, buffer)

        result = ModelTrainingResult(
            corridor_id=int(corridor_frame["corridor_id"].iloc[0]),
            corridor_name=str(corridor_frame["corridor_name"].iloc[0]),
            model_name=self.model_name,
            training_strategy=self.training_strategy,
            params=params,
            metrics=metrics,
            forecasts=build_forecast_frame(records),
            model_object=future_model,
            artifacts={"model_pickle": model_path},
        )
        result.validate_forecast_schema()
        return result
