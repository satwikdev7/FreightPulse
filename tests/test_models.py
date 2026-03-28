import pandas as pd

from src.models.arima_model import ArimaForecaster
from src.models.base import ForecastRecord
from src.models.common import build_forecast_frame, extrapolate_series
from src.models.prophet_model import ProphetForecaster


def test_build_forecast_frame_uses_common_schema():
    frame = build_forecast_frame(
        [
            ForecastRecord(
                corridor_id=1,
                corridor_name="LA_to_Chicago",
                model_name="ARIMA",
                training_strategy="leave_last_year_out",
                forecast_type="holdout",
                year=2024,
                prediction=100.0,
                lower_bound=95.0,
                upper_bound=105.0,
            )
        ]
    )
    assert frame.columns.tolist() == [
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


def test_extrapolate_series_extends_last_trend():
    series = pd.Series([100.0, 105.0], index=[2024, 2025])
    future = extrapolate_series(series, [2025, 2026, 2027])
    assert future[2025] == 105.0
    assert future[2026] == 110.0
    assert future[2027] == 115.0


def test_arima_model_smoke_train():
    corridor = pd.DataFrame(
        {
            "corridor_id": [1] * 8,
            "corridor_name": ["LA_to_Chicago"] * 8,
            "year": [2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024],
            "total_tons": [100.0, 101.0, 102.0, 101.5, 103.0, 104.0, 105.0, 106.0],
            "total_value": [1000.0] * 8,
            "total_tmiles": [2000.0] * 8,
        }
    )
    result = ArimaForecaster().train(corridor)
    assert result.model_name == "ARIMA"
    assert not result.forecasts.empty


def test_prophet_model_smoke_train():
    corridor = pd.DataFrame(
        {
            "corridor_id": [1] * 8,
            "corridor_name": ["LA_to_Chicago"] * 8,
            "year": [2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024],
            "total_tons": [100.0, 101.0, 102.0, 101.5, 103.0, 104.0, 105.0, 106.0],
            "gdp": [1000.0, 1010.0, 1020.0, 1015.0, 1030.0, 1040.0, 1050.0, 1060.0],
            "diesel_price": [2.0, 2.1, 2.2, 2.0, 2.3, 2.5, 2.4, 2.3],
            "industrial_production": [90.0, 91.0, 92.0, 91.0, 93.0, 94.0, 95.0, 96.0],
            "unemployment_rate": [5.0, 4.9, 4.8, 7.0, 6.0, 5.0, 4.5, 4.3],
        }
    )
    annual_fred = pd.DataFrame(
        {
            "year": [2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026, 2027, 2028, 2029, 2030],
            "gdp": [1000.0, 1010.0, 1020.0, 1015.0, 1030.0, 1040.0, 1050.0, 1060.0, 1070.0, 1080.0, 1090.0, 1100.0, 1110.0, 1120.0],
            "diesel_price": [2.0, 2.1, 2.2, 2.0, 2.3, 2.5, 2.4, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 2.9],
            "industrial_production": [90.0, 91.0, 92.0, 91.0, 93.0, 94.0, 95.0, 96.0, 97.0, 98.0, 99.0, 100.0, 101.0, 102.0],
            "unemployment_rate": [5.0, 4.9, 4.8, 7.0, 6.0, 5.0, 4.5, 4.3, 4.2, 4.1, 4.0, 3.9, 3.8, 3.7],
            "ppi_transportation": [100.0] * 14,
            "vehicle_miles": [10000.0] * 14,
        }
    )
    result = ProphetForecaster().train(corridor, annual_fred)
    assert result.model_name == "Prophet"
    assert not result.forecasts.empty
