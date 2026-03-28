from src.evaluation.metrics import compute_regression_metrics, mean_absolute_percentage_error


def test_mape_computation():
    value = mean_absolute_percentage_error([100.0, 200.0], [90.0, 220.0])
    assert round(value, 2) == 10.0


def test_regression_metrics_bundle():
    metrics = compute_regression_metrics([100.0, 120.0], [110.0, 115.0])
    assert set(metrics) == {"mape", "rmse", "mae"}
    assert metrics["mae"] > 0
