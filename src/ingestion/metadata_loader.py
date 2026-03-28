from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from src.utils.config import CONFIG


@dataclass(frozen=True)
class MetadataSheetConfig:
    sheet_name: str
    target_table: str
    rename_map: dict[str, str]


METADATA_SHEETS: tuple[MetadataSheetConfig, ...] = (
    MetadataSheetConfig(
        sheet_name="State",
        target_table="raw_faf_metadata_states",
        rename_map={
            "Numeric Label": "numeric_label",
            "Description": "description",
        },
    ),
    MetadataSheetConfig(
        sheet_name="FAF Zone (Domestic)",
        target_table="raw_faf_metadata_zones",
        rename_map={
            "Numeric Label": "numeric_label",
            "Short Description": "short_description",
            "Long Description": "long_description",
        },
    ),
    MetadataSheetConfig(
        sheet_name="Commodity (SCTG2)",
        target_table="raw_faf_metadata_commodities",
        rename_map={
            "Numeric Label": "numeric_label",
            "Description": "description",
        },
    ),
    MetadataSheetConfig(
        sheet_name="Mode",
        target_table="raw_faf_metadata_modes",
        rename_map={
            "Numeric Label": "numeric_label",
            "Description": "description",
        },
    ),
    MetadataSheetConfig(
        sheet_name="Trade Type",
        target_table="raw_faf_metadata_trade_type",
        rename_map={
            "Numeric Label": "numeric_label",
            "Description": "description",
        },
    ),
    MetadataSheetConfig(
        sheet_name="Distance Band",
        target_table="raw_faf_metadata_distance_band",
        rename_map={
            "Numeric Label": "numeric_label",
            "Description": "description",
        },
    ),
)


class MetadataLoader:
    """Read the FAF metadata workbook and normalize the lookup tabs we need."""

    def __init__(self, metadata_path: Path | None = None) -> None:
        self.metadata_path = Path(metadata_path or CONFIG.faf_hilo_metadata_xlsx)

    def _read_sheet(self, config: MetadataSheetConfig) -> pd.DataFrame:
        frame = pd.read_excel(self.metadata_path, sheet_name=config.sheet_name, engine="openpyxl")
        normalized = frame.rename(columns=config.rename_map).loc[:, list(config.rename_map.values())]
        return normalized

    def load_all(self) -> dict[str, pd.DataFrame]:
        return {sheet.target_table: self._read_sheet(sheet) for sheet in METADATA_SHEETS}
