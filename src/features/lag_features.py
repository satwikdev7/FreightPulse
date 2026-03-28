from __future__ import annotations

import pandas as pd


def add_lag_features(frame: pd.DataFrame) -> pd.DataFrame:
    """Add lagged corridor features required by the feature store."""
    enriched = frame.copy()
    grouped = enriched.groupby("corridor_id", group_keys=False)
    enriched["tons_lag1"] = grouped["total_tons"].shift(1)
    enriched["tons_lag2"] = grouped["total_tons"].shift(2)
    enriched["tons_lag3"] = grouped["total_tons"].shift(3)
    enriched["value_lag1"] = grouped["total_value"].shift(1)
    enriched["diesel_lag1"] = grouped["diesel_price"].shift(1)
    return enriched
