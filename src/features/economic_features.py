from __future__ import annotations

import numpy as np
import pandas as pd


def add_economic_change_features(frame: pd.DataFrame) -> pd.DataFrame:
    """Add year-over-year macro change signals."""
    enriched = frame.copy()
    grouped = enriched.groupby("corridor_id", group_keys=False)
    enriched["gdp_yoy_change"] = grouped["gdp"].pct_change()
    enriched["diesel_yoy_change"] = grouped["diesel_price"].pct_change()
    enriched = enriched.replace([np.inf, -np.inf], np.nan)
    return enriched
