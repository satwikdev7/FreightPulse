from __future__ import annotations

import pandas as pd


def add_rolling_features(frame: pd.DataFrame) -> pd.DataFrame:
    """Add rolling statistics for corridor tonnage."""
    enriched = frame.copy()
    grouped = enriched.groupby("corridor_id", group_keys=False)["total_tons"]
    enriched["tons_rolling_mean_3yr"] = grouped.transform(
        lambda series: series.rolling(window=3, min_periods=1).mean()
    )
    enriched["tons_rolling_std_3yr"] = grouped.transform(
        lambda series: series.rolling(window=3, min_periods=2).std()
    )
    return enriched
