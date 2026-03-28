import pandas as pd

from src.evaluation.bts_benchmark import evaluate_bts_benchmark
from src.evaluation.holdout import build_expanding_window_splits, evaluate_holdout
from src.evaluation.model_comparison import build_comparison_table


def test_holdout_evaluation_scores_single_prediction(tmp_path):
    forecasts = pd.DataFrame(
        [
            {
                "corridor_id": 1,
                "corridor_name": "LA_to_Chicago",
                "model_name": "ARIMA",
                "training_strategy": "leave_last_year_out",
                "forecast_type": "holdout",
                "year": 2024,
                "prediction": 100.0,
                "lower_bound": 95.0,
                "upper_bound": 105.0,
            }
        ]
    )
    actuals = pd.DataFrame(
        [{"corridor_id": 1, "corridor_name": "LA_to_Chicago", "year": 2024, "total_tons": 110.0}]
    )
    result = evaluate_holdout(forecasts, actuals, output_path=tmp_path / "holdout.csv")
    assert result.scores["mape"].iloc[0] > 0
    assert result.output_path.exists()


def test_bts_benchmark_marks_band_membership(tmp_path):
    forecasts = pd.DataFrame(
        [
            {
                "corridor_id": 1,
                "corridor_name": "LA_to_Chicago",
                "model_name": "ARIMA",
                "training_strategy": "leave_last_year_out",
                "forecast_type": "future",
                "year": 2030,
                "prediction": 150.0,
                "lower_bound": 140.0,
                "upper_bound": 160.0,
            }
        ]
    )
    bts = pd.DataFrame(
        [
            {
                "corridor_id": 1,
                "corridor_name": "LA_to_Chicago",
                "year": 2030,
                "bts_tons": 155.0,
                "bts_tons_low": 145.0,
                "bts_tons_high": 165.0,
            }
        ]
    )
    result = evaluate_bts_benchmark(forecasts, bts, output_path=tmp_path / "bts.csv")
    assert bool(result.scores["within_bts_band"].iloc[0]) is True


def test_model_comparison_combines_holdout_and_bts(tmp_path):
    holdout = pd.DataFrame(
        [
            {
                "corridor_id": 1,
                "corridor_name": "LA_to_Chicago",
                "model_name": "ARIMA",
                "training_strategy": "leave_last_year_out",
                "mape": 5.0,
                "rmse": 10.0,
                "mae": 10.0,
            }
        ]
    )
    bts = pd.DataFrame(
        [
            {
                "corridor_id": 1,
                "corridor_name": "LA_to_Chicago",
                "model_name": "ARIMA",
                "training_strategy": "leave_last_year_out",
                "within_bts_band": True,
                "pct_deviation_vs_bts": -2.0,
            }
        ]
    )
    comparison, output_path = build_comparison_table(holdout, bts, output_path=tmp_path / "comparison.csv")
    assert len(comparison) == 1
    assert output_path.exists()


def test_expanding_window_split_builder():
    splits = build_expanding_window_splits()
    assert splits[0]["train_years"] == [2017, 2018, 2019, 2020]
    assert splits[-1]["test_year"] == 2024
