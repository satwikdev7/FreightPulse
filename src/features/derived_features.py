from __future__ import annotations

import numpy as np
import pandas as pd


def add_derived_features(frame: pd.DataFrame) -> pd.DataFrame:
    """Add YoY and ratio features used by downstream models."""
    enriched = frame.copy()
    grouped = enriched.groupby("corridor_id", group_keys=False)
    enriched["tons_yoy_change"] = grouped["total_tons"].pct_change()
    enriched["value_per_ton"] = enriched["total_value"] / enriched["total_tons"]
    enriched["tmiles_per_ton"] = enriched["total_tmiles"] / enriched["total_tons"]
    enriched = enriched.replace([np.inf, -np.inf], np.nan)
    return enriched
