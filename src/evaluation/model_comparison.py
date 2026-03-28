from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from src.evaluation.bts_benchmark import BTSBenchmarkResult, evaluate_bts_benchmark
from src.evaluation.holdout import HoldoutEvaluationResult, evaluate_holdout
from src.models.forecasting import ForecastingService
from src.storage.db_manager import DuckDBManager
from src.utils import paths


@dataclass(frozen=True)
class ComparisonArtifacts:
    holdout: HoldoutEvaluationResult
    bts: BTSBenchmarkResult
    comparison_table: pd.DataFrame
    comparison_path: Path


def load_forecast_artifacts(forecasts_dir: Path | None = None) -> pd.DataFrame:
    directory = forecasts_dir or paths.PROCESSED_FORECASTS_DIR
    frames: list[pd.DataFrame] = []
    for csv_path in sorted(directory.glob("*_corridor_*_forecasts.csv")):
        frames.append(pd.read_csv(csv_path))
    if not frames:
        raise FileNotFoundError("No forecast CSVs found. Run training before evaluation.")
    return pd.concat(frames, ignore_index=True)


def build_comparison_table(
    holdout_scores: pd.DataFrame,
    bts_scores: pd.DataFrame,
    output_path: Path | None = None,
) -> tuple[pd.DataFrame, Path]:
    merged = holdout_scores.merge(
        bts_scores[
            [
                "corridor_id",
                "corridor_name",
                "model_name",
                "training_strategy",
                "within_bts_band",
                "pct_deviation_vs_bts",
            ]
        ],
        on=["corridor_id", "corridor_name", "model_name", "training_strategy"],
        how="left",
    )
    comparison = merged[
        [
            "corridor_id",
            "corridor_name",
            "model_name",
            "training_strategy",
            "mape",
            "rmse",
            "mae",
            "within_bts_band",
            "pct_deviation_vs_bts",
        ]
    ].sort_values(["corridor_id", "model_name"]).reset_index(drop=True)

    destination = output_path or (paths.PROCESSED_EVALUATION_DIR / "model_comparison.csv")
    destination.parent.mkdir(parents=True, exist_ok=True)
    comparison.to_csv(destination, index=False)
    return comparison, destination


def run_full_evaluation(
    db_manager: DuckDBManager | None = None,
    retrain_if_missing: bool = False,
) -> ComparisonArtifacts:
    manager = db_manager or DuckDBManager()
    try:
        forecasts = load_forecast_artifacts()
    except FileNotFoundError:
        if not retrain_if_missing:
            raise
        ForecastingService(db_manager=manager).train_all_models(log_to_mlflow=True)
        forecasts = load_forecast_artifacts()

    actuals = manager.fetch_df("SELECT * FROM feature_corridor_annual ORDER BY corridor_id, year")
    bts = manager.fetch_df("SELECT * FROM feature_bts_forecast ORDER BY corridor_id, year")

    holdout = evaluate_holdout(forecasts, actuals)
    benchmark = evaluate_bts_benchmark(forecasts, bts)
    comparison, comparison_path = build_comparison_table(holdout.scores, benchmark.scores)

    return ComparisonArtifacts(
        holdout=holdout,
        bts=benchmark,
        comparison_table=comparison,
        comparison_path=comparison_path,
    )
