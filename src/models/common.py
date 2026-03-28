from __future__ import annotations

from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

from src.models.base import FORECAST_COLUMNS, ForecastRecord
from src.utils import paths


ARIMA_PROPHET_HORIZON = [2025, 2026, 2027, 2028, 2029, 2030]
HOLDOUT_YEAR = 2024
TRAIN_END_YEAR = 2023


def ensure_model_output_dirs() -> tuple[Path, Path]:
    models_dir = paths.PROCESSED_DIR / "models"
    forecasts_dir = paths.PROCESSED_FORECASTS_DIR
    models_dir.mkdir(parents=True, exist_ok=True)
    forecasts_dir.mkdir(parents=True, exist_ok=True)
    return models_dir, forecasts_dir


def build_forecast_frame(records: Iterable[ForecastRecord]) -> pd.DataFrame:
    frame = pd.DataFrame([record.to_dict() for record in records])
    if frame.empty:
        return pd.DataFrame(columns=FORECAST_COLUMNS)
    return frame[FORECAST_COLUMNS]


def extrapolate_series(values_by_year: pd.Series, target_years: list[int]) -> dict[int, float]:
    clean = values_by_year.dropna().sort_index()
    if clean.empty:
        return {year: float("nan") for year in target_years}

    result: dict[int, float] = {int(year): float(value) for year, value in clean.items()}
    if len(clean) >= 2:
        slope = float(clean.iloc[-1] - clean.iloc[-2])
    else:
        slope = 0.0

    last_year = int(clean.index[-1])
    last_value = float(clean.iloc[-1])
    for year in sorted(target_years):
        if year in result:
            continue
        step = year - last_year
        result[year] = last_value + (slope * step)
    return {year: float(result[year]) for year in target_years}


def build_future_regressors(
    corridor_frame: pd.DataFrame,
    annual_fred_frame: pd.DataFrame,
    target_years: list[int],
) -> pd.DataFrame:
    regressor_columns = [
        "gdp",
        "diesel_price",
        "industrial_production",
        "unemployment_rate",
        "ppi_transportation",
        "vehicle_miles",
    ]
    future = pd.DataFrame({"year": target_years})

    for column in regressor_columns:
        source = annual_fred_frame.set_index("year")[column]
        extrapolated = extrapolate_series(source, target_years)
        future[column] = future["year"].map(extrapolated)

    corridor_name = corridor_frame["corridor_name"].iloc[0]
    corridor_id = int(corridor_frame["corridor_id"].iloc[0])
    future["corridor_id"] = corridor_id
    future["corridor_name"] = corridor_name
    return future


def percentile_interval(predictions: list[float], width: float = 0.8) -> tuple[float, float]:
    alpha = (1.0 - width) / 2.0
    lower = float(np.quantile(predictions, alpha))
    upper = float(np.quantile(predictions, 1.0 - alpha))
    return lower, upper
