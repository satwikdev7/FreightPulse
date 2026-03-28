from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path

import pandas as pd


FORECAST_COLUMNS = [
    "corridor_id",
    "corridor_name",
    "model_name",
    "training_strategy",
    "forecast_type",
    "year",
    "prediction",
    "lower_bound",
    "upper_bound",
]


@dataclass(frozen=True)
class ForecastRecord:
    corridor_id: int
    corridor_name: str
    model_name: str
    training_strategy: str
    forecast_type: str
    year: int
    prediction: float
    lower_bound: float | None = None
    upper_bound: float | None = None

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class ModelTrainingResult:
    corridor_id: int
    corridor_name: str
    model_name: str
    training_strategy: str
    params: dict[str, object]
    metrics: dict[str, float]
    forecasts: pd.DataFrame
    model_object: object
    artifacts: dict[str, Path] = field(default_factory=dict)

    def validate_forecast_schema(self) -> None:
        missing = [column for column in FORECAST_COLUMNS if column not in self.forecasts.columns]
        if missing:
            raise ValueError(f"Forecast output missing columns: {missing}")
