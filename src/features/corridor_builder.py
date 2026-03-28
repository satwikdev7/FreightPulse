from __future__ import annotations

from typing import Iterable

import pandas as pd

from src.utils.corridor_config import CorridorDefinition, LOCKED_CORRIDORS


def corridor_definitions_frame(
    corridors: Iterable[CorridorDefinition] = LOCKED_CORRIDORS,
) -> pd.DataFrame:
    """Return the locked corridor definitions as a small DataFrame."""
    return pd.DataFrame(
        [
            {
                "corridor_id": corridor.corridor_id,
                "corridor_name": corridor.name,
                "origin": corridor.origin,
                "destination": corridor.destination,
            }
            for corridor in corridors
        ]
    )
