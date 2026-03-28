from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CorridorDefinition:
    corridor_id: int
    name: str
    origin: int
    destination: int


LOCKED_CORRIDORS: tuple[CorridorDefinition, ...] = (
    CorridorDefinition(1, "LA_to_Chicago", 61, 171),
    CorridorDefinition(2, "Houston_to_Atlanta", 481, 131),
    CorridorDefinition(3, "NYC_to_Miami", 362, 121),
    CorridorDefinition(4, "Dallas_to_Memphis", 482, 471),
    CorridorDefinition(5, "Seattle_to_Portland", 531, 411),
)


CORRIDOR_NAME_TO_ID = {corridor.name: corridor.corridor_id for corridor in LOCKED_CORRIDORS}
