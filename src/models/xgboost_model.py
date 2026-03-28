from __future__ import annotations

import pickle

import numpy as np
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
    percentile_interval,
)


XGB_FEATURE_COLUMNS = [
    "corridor_id",
    "year",
    "gdp",
    "diesel_price",
    "industrial_production",
    "unemployment_rate",
    "ppi_transportation",
    "vehicle_miles",
    "tons_lag1",
    "tons_lag2",
    "tons_lag3",
    "value_lag1",
    "diesel_lag1",
    "tons_rolling_mean_3yr",
    "tons_rolling_std_3yr",
    "gdp_yoy_change",
    "diesel_yoy_change",
]


class XGBoostForecaster:
    model_name = "XGBoost"
    training_strategy = "pooled_leave_last_year_out"

    def __init__(
        self,
        n_estimators: int = 200,
        max_depth: int = 3,
        learning_rate: float = 0.05,
    ) -> None:
        self.n_estimators = n_estimators
        self.max_depth = max_depth
        self.learning_rate = learning_rate

    def _build_model(self):
        from xgboost import XGBRegressor

        return XGBRegressor(
            n_estimators=self.n_estimators,
            max_depth=self.max_depth,
            learning_rate=self.learning_rate,
            objective="reg:squarederror",
            subsample=1.0,
            colsample_bytree=1.0,
            min_child_weight=1,
            random_state=42,
        )

    @staticmethod
    def _training_frame(feature_frame: pd.DataFrame) -> pd.DataFrame:
        frame = feature_frame.copy().sort_values(["corridor_id", "year"]).reset_index(drop=True)
        return frame.dropna(subset=XGB_FEATURE_COLUMNS + ["total_tons"])

    @staticmethod
    def _build_row_from_history(
        history: pd.DataFrame,
        corridor_id: int,
        corridor_name: str,
        year: int,
        regressors: dict[str, float],
        value_ratio: float,
    ) -> dict[str, float]:
        corridor_history = history[history["corridor_id"] == corridor_id].sort_values("year").reset_index(drop=True)
        last1 = corridor_history.iloc[-1]
        last2 = corridor_history.iloc[-2]
        last3 = corridor_history.iloc[-3]
        estimated_prev_value = float(last1["total_tons"]) * float(value_ratio)
        tons_yoy = (float(last1["total_tons"]) - float(last2["total_tons"])) / float(last2["total_tons"])

        return {
            "corridor_id": corridor_id,
            "corridor_name": corridor_name,
            "year": year,
            "gdp": regressors["gdp"],
            "diesel_price": regressors["diesel_price"],
            "industrial_production": regressors["industrial_production"],
            "unemployment_rate": regressors["unemployment_rate"],
            "ppi_transportation": regressors["ppi_transportation"],
            "vehicle_miles": regressors["vehicle_miles"],
            "tons_lag1": float(last1["total_tons"]),
            "tons_lag2": float(last2["total_tons"]),
            "tons_lag3": float(last3["total_tons"]),
            "value_lag1": estimated_prev_value,
            "diesel_lag1": float(last1["diesel_price"]),
            "tons_rolling_mean_3yr": float(
                np.mean([last1["total_tons"], last2["total_tons"], last3["total_tons"]])
            ),
            "tons_rolling_std_3yr": float(
                np.std([last1["total_tons"], last2["total_tons"], last3["total_tons"]], ddof=1)
            ),
            "gdp_yoy_change": (
                (regressors["gdp"] - float(last1["gdp"])) / float(last1["gdp"]) if last1["gdp"] else np.nan
            ),
            "diesel_yoy_change": (
                (regressors["diesel_price"] - float(last1["diesel_price"])) / float(last1["diesel_price"])
                if last1["diesel_price"]
                else np.nan
            ),
            "total_value": estimated_prev_value,
            "total_tmiles": float(last1["total_tons"]) * float(last1.get("tmiles_per_ton", 0.0) or 0.0),
            "diesel_price_history": regressors["diesel_price"],
            "tons_yoy_change": tons_yoy,
        }

    def train(
        self,
        feature_frame: pd.DataFrame,
        annual_fred_frame: pd.DataFrame,
    ) -> list[ModelTrainingResult]:
        training_frame = self._training_frame(feature_frame)
        train_rows = training_frame[training_frame["year"] <= TRAIN_END_YEAR].copy()
        holdout_rows = training_frame[training_frame["year"] == HOLDOUT_YEAR].copy()

        model = self._build_model()
        model.fit(train_rows[XGB_FEATURE_COLUMNS], train_rows["total_tons"])
        holdout_predictions = model.predict(holdout_rows[XGB_FEATURE_COLUMNS])

        full_model = self._build_model()
        full_model.fit(training_frame[XGB_FEATURE_COLUMNS], training_frame["total_tons"])

        future_regressors = build_future_regressors(
            feature_frame[["corridor_id", "corridor_name", "year"]].drop_duplicates(),
            annual_fred_frame,
            ARIMA_PROPHET_HORIZON,
        )

        history = feature_frame[
            [
                "corridor_id",
                "corridor_name",
                "year",
                "total_tons",
                "gdp",
                "diesel_price",
                "tmiles_per_ton",
                "value_per_ton",
            ]
        ].copy()
        forecasts_by_corridor: dict[int, list[ForecastRecord]] = {}
        holdout_metrics_by_corridor: dict[int, dict[str, float]] = {}

        for corridor_id in sorted(feature_frame["corridor_id"].unique()):
            corridor_hist = feature_frame[feature_frame["corridor_id"] == corridor_id].sort_values("year")
            corridor_name = str(corridor_hist["corridor_name"].iloc[0])
            ratio = float(corridor_hist["value_per_ton"].dropna().mean())
            holdout_row = holdout_rows[holdout_rows["corridor_id"] == corridor_id].iloc[0]
            holdout_pred = float(
                holdout_predictions[holdout_rows.index.get_loc(holdout_row.name)]
            )
            holdout_actual = float(holdout_row["total_tons"])
            holdout_metrics_by_corridor[corridor_id] = compute_regression_metrics(
                [holdout_actual], [holdout_pred]
            )

            records = [
                ForecastRecord(
                    corridor_id=int(corridor_id),
                    corridor_name=corridor_name,
                    model_name=self.model_name,
                    training_strategy=self.training_strategy,
                    forecast_type="holdout",
                    year=HOLDOUT_YEAR,
                    prediction=holdout_pred,
                    lower_bound=holdout_pred * 0.9,
                    upper_bound=holdout_pred * 1.1,
                )
            ]

            corridor_future_history = history[history["corridor_id"] == corridor_id].copy()
            for year in ARIMA_PROPHET_HORIZON:
                regressors = annual_fred_frame[annual_fred_frame["year"] == year]
                if regressors.empty:
                    generated = build_future_regressors(corridor_hist, annual_fred_frame, [year]).iloc[0]
                else:
                    generated = regressors.iloc[0]
                feature_row = self._build_row_from_history(
                    history=corridor_future_history,
                    corridor_id=int(corridor_id),
                    corridor_name=corridor_name,
                    year=year,
                    regressors={
                        "gdp": float(generated["gdp"]),
                        "diesel_price": float(generated["diesel_price"]),
                        "industrial_production": float(generated["industrial_production"]),
                        "unemployment_rate": float(generated["unemployment_rate"]),
                        "ppi_transportation": float(generated["ppi_transportation"]),
                        "vehicle_miles": float(generated["vehicle_miles"]),
                    },
                    value_ratio=ratio,
                )
                feature_df = pd.DataFrame([feature_row])[XGB_FEATURE_COLUMNS]
                future_prediction = float(full_model.predict(feature_df)[0])
                interval = percentile_interval(
                    [
                        future_prediction * 0.95,
                        future_prediction,
                        future_prediction * 1.05,
                    ]
                )
                records.append(
                    ForecastRecord(
                        corridor_id=int(corridor_id),
                        corridor_name=corridor_name,
                        model_name=self.model_name,
                        training_strategy=self.training_strategy,
                        forecast_type="future",
                        year=year,
                        prediction=future_prediction,
                        lower_bound=interval[0],
                        upper_bound=interval[1],
                    )
                )
                corridor_future_history = pd.concat(
                    [
                        corridor_future_history,
                        pd.DataFrame(
                            [
                                {
                                    "corridor_id": corridor_id,
                                    "corridor_name": corridor_name,
                                    "year": year,
                                    "total_tons": future_prediction,
                                    "gdp": feature_row["gdp"],
                                    "diesel_price": feature_row["diesel_price"],
                                    "tmiles_per_ton": float(
                                        corridor_hist["tmiles_per_ton"].dropna().mean()
                                    ),
                                    "value_per_ton": ratio,
                                }
                            ]
                        ),
                    ],
                    ignore_index=True,
                )

            forecasts_by_corridor[corridor_id] = records

        models_dir, forecasts_dir = ensure_model_output_dirs()
        model_path = models_dir / "xgboost_pooled.pkl"
        with model_path.open("wb") as buffer:
            pickle.dump(full_model, buffer)

        feature_importance_path = forecasts_dir / "xgboost_feature_importance.csv"
        importance_frame = pd.DataFrame(
            {"feature": XGB_FEATURE_COLUMNS, "importance": full_model.feature_importances_}
        ).sort_values("importance", ascending=False)
        importance_frame.to_csv(feature_importance_path, index=False)

        results: list[ModelTrainingResult] = []
        for corridor_id, records in forecasts_by_corridor.items():
            corridor_name = str(feature_frame.loc[feature_frame["corridor_id"] == corridor_id, "corridor_name"].iloc[0])
            result = ModelTrainingResult(
                corridor_id=int(corridor_id),
                corridor_name=corridor_name,
                model_name=self.model_name,
                training_strategy=self.training_strategy,
                params={
                    "n_estimators": self.n_estimators,
                    "max_depth": self.max_depth,
                    "learning_rate": self.learning_rate,
                    "pooled_training_rows": int(len(train_rows)),
                },
                metrics=holdout_metrics_by_corridor[corridor_id],
                forecasts=build_forecast_frame(records),
                model_object=full_model,
                artifacts={
                    "model_pickle": model_path,
                    "feature_importance_csv": feature_importance_path,
                },
            )
            result.validate_forecast_schema()
            results.append(result)
        return results
