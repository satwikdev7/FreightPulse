from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from src.utils import paths


@dataclass(frozen=True)
class BTSBenchmarkResult:
    scores: pd.DataFrame
    output_path: Path


def evaluate_bts_benchmark(
    forecasts: pd.DataFrame,
    bts_forecasts: pd.DataFrame,
    benchmark_year: int = 2030,
    output_path: Path | None = None,
) -> BTSBenchmarkResult:
    model_2030 = forecasts[
        (forecasts["forecast_type"] == "future") & (forecasts["year"] == benchmark_year)
    ].copy()
    bts_2030 = bts_forecasts[bts_forecasts["year"] == benchmark_year].copy()

    scored = model_2030.merge(
        bts_2030,
        on=["corridor_id", "corridor_name", "year"],
        how="inner",
    )
    scored["within_bts_band"] = scored.apply(
        lambda row: row["bts_tons_low"] <= row["prediction"] <= row["bts_tons_high"],
        axis=1,
    )
    scored["pct_deviation_vs_bts"] = ((scored["prediction"] - scored["bts_tons"]) / scored["bts_tons"]) * 100

    score_frame = scored[
        [
            "corridor_id",
            "corridor_name",
            "model_name",
            "training_strategy",
            "year",
            "prediction",
            "bts_tons",
            "bts_tons_low",
            "bts_tons_high",
            "within_bts_band",
            "pct_deviation_vs_bts",
        ]
    ].sort_values(["corridor_id", "model_name"]).reset_index(drop=True)

    destination = output_path or (paths.PROCESSED_EVALUATION_DIR / "bts_benchmark_scores.csv")
    destination.parent.mkdir(parents=True, exist_ok=True)
    score_frame.to_csv(destination, index=False)
    return BTSBenchmarkResult(scores=score_frame, output_path=destination)
