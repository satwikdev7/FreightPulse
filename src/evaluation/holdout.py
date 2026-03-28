from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from src.evaluation.metrics import compute_regression_metrics
from src.utils import paths


@dataclass(frozen=True)
class HoldoutEvaluationResult:
    scores: pd.DataFrame
    output_path: Path


def evaluate_holdout(
    forecasts: pd.DataFrame,
    actuals: pd.DataFrame,
    output_path: Path | None = None,
) -> HoldoutEvaluationResult:
    holdout_forecasts = forecasts[forecasts["forecast_type"] == "holdout"].copy()
    actual_2024 = actuals[actuals["year"] == 2024][
        ["corridor_id", "corridor_name", "year", "total_tons"]
    ].rename(columns={"total_tons": "actual"})

    scored = holdout_forecasts.merge(
        actual_2024,
        on=["corridor_id", "corridor_name", "year"],
        how="inner",
    )

    rows: list[dict[str, object]] = []
    for _, row in scored.iterrows():
        metrics = compute_regression_metrics([row["actual"]], [row["prediction"]])
        rows.append(
            {
                "corridor_id": int(row["corridor_id"]),
                "corridor_name": row["corridor_name"],
                "model_name": row["model_name"],
                "training_strategy": row["training_strategy"],
                "year": int(row["year"]),
                "actual": float(row["actual"]),
                "prediction": float(row["prediction"]),
                **metrics,
            }
        )

    score_frame = pd.DataFrame(rows).sort_values(["corridor_id", "model_name"]).reset_index(drop=True)
    destination = output_path or (paths.PROCESSED_EVALUATION_DIR / "holdout_scores.csv")
    destination.parent.mkdir(parents=True, exist_ok=True)
    score_frame.to_csv(destination, index=False)
    return HoldoutEvaluationResult(scores=score_frame, output_path=destination)


def build_expanding_window_splits(
    years: list[int] | None = None,
    min_train_size: int = 4,
) -> list[dict[str, list[int] | int]]:
    ordered_years = years or [2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]
    splits: list[dict[str, list[int] | int]] = []
    for index in range(min_train_size, len(ordered_years)):
        splits.append(
            {
                "train_years": ordered_years[:index],
                "test_year": ordered_years[index],
            }
        )
    return splits
