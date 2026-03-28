import pandas as pd

from src.features.derived_features import add_derived_features
from src.features.economic_features import add_economic_change_features
from src.features.lag_features import add_lag_features
from src.features.rolling_features import add_rolling_features
from src.storage.staged_layer import _infer_state_name


def test_feature_helpers_add_expected_columns():
    frame = pd.DataFrame(
        {
            "corridor_id": [1, 1, 1],
            "corridor_name": ["LA_to_Chicago"] * 3,
            "year": [2017, 2018, 2019],
            "total_tons": [100.0, 110.0, 120.0],
            "total_value": [200.0, 220.0, 240.0],
            "total_tmiles": [300.0, 330.0, 360.0],
            "gdp": [1000.0, 1030.0, 1060.0],
            "diesel_price": [2.0, 2.2, 2.4],
            "industrial_production": [90.0, 91.0, 92.0],
            "unemployment_rate": [5.0, 4.8, 4.6],
            "ppi_transportation": [110.0, 111.0, 112.0],
            "vehicle_miles": [10000.0, 10100.0, 10200.0],
        }
    )

    enriched = add_lag_features(frame)
    enriched = add_rolling_features(enriched)
    enriched = add_derived_features(enriched)
    enriched = add_economic_change_features(enriched)

    assert "tons_lag1" in enriched.columns
    assert "tons_rolling_mean_3yr" in enriched.columns
    assert "tons_yoy_change" in enriched.columns
    assert "gdp_yoy_change" in enriched.columns
    assert "value_per_ton" in enriched.columns


def test_infer_state_name_handles_core_zone_patterns():
    assert _infer_state_name("Los Angeles CA", "Los Angeles-Long Beach, CA  CFS Area") == "California"
    assert _infer_state_name("Rest of TX", "Remainder of Texas") == "Texas"
    assert _infer_state_name("Alaska", "Alaska") == "Alaska"
    assert (
        _infer_state_name(
            "Philadelphia PA-NJ-DE-MD (DE Part)",
            "Philadelphia-Reading-Camden, PA-NJ-DE-MD  CFS Area (DE Part)",
        )
        == "Delaware"
    )
