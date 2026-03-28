from __future__ import annotations

import re
from dataclasses import dataclass

import pandas as pd

from src.storage.db_manager import DuckDBManager
from src.storage.schema_manager import initialize_database


STATE_ABBREVIATIONS: dict[str, str] = {
    "Alabama": "AL",
    "Alaska": "AK",
    "Arizona": "AZ",
    "Arkansas": "AR",
    "California": "CA",
    "Colorado": "CO",
    "Connecticut": "CT",
    "Delaware": "DE",
    "District of Columbia": "DC",
    "Florida": "FL",
    "Georgia": "GA",
    "Hawaii": "HI",
    "Idaho": "ID",
    "Illinois": "IL",
    "Indiana": "IN",
    "Iowa": "IA",
    "Kansas": "KS",
    "Kentucky": "KY",
    "Louisiana": "LA",
    "Maine": "ME",
    "Maryland": "MD",
    "Massachusetts": "MA",
    "Michigan": "MI",
    "Minnesota": "MN",
    "Mississippi": "MS",
    "Missouri": "MO",
    "Montana": "MT",
    "Nebraska": "NE",
    "Nevada": "NV",
    "New Hampshire": "NH",
    "New Jersey": "NJ",
    "New Mexico": "NM",
    "New York": "NY",
    "North Carolina": "NC",
    "North Dakota": "ND",
    "Ohio": "OH",
    "Oklahoma": "OK",
    "Oregon": "OR",
    "Pennsylvania": "PA",
    "Rhode Island": "RI",
    "South Carolina": "SC",
    "South Dakota": "SD",
    "Tennessee": "TN",
    "Texas": "TX",
    "Utah": "UT",
    "Vermont": "VT",
    "Virginia": "VA",
    "Washington": "WA",
    "West Virginia": "WV",
    "Wisconsin": "WI",
    "Wyoming": "WY",
}
ABBREVIATION_TO_STATE = {abbr: name for name, abbr in STATE_ABBREVIATIONS.items()}


@dataclass(frozen=True)
class TableBuildResult:
    table_name: str
    row_count: int


def _infer_state_name(short_description: str, long_description: str) -> str | None:
    part_match = re.search(r"\(([A-Z]{2}) Part\)", f"{short_description} {long_description}")
    if part_match:
        part_abbr = part_match.group(1)
        if part_abbr in ABBREVIATION_TO_STATE:
            return ABBREVIATION_TO_STATE[part_abbr]

    if short_description in STATE_ABBREVIATIONS:
        return short_description

    if long_description.startswith("Remainder of "):
        candidate = long_description.removeprefix("Remainder of ").strip()
        if candidate in STATE_ABBREVIATIONS:
            return candidate

    if short_description.startswith("Rest of "):
        candidate_abbr = short_description.removeprefix("Rest of ").strip()
        return ABBREVIATION_TO_STATE.get(candidate_abbr)

    if long_description in STATE_ABBREVIATIONS:
        return long_description

    text = f"{short_description} {long_description}"
    matches = re.findall(r"\b([A-Z]{2})\b", text)
    for match in reversed(matches):
        if match in ABBREVIATION_TO_STATE:
            return ABBREVIATION_TO_STATE[match]

    return None


class StagedLayerBuilder:
    """Build the project staged tables from the raw DuckDB layer."""

    def __init__(self, db_manager: DuckDBManager | None = None) -> None:
        self.db_manager = db_manager or DuckDBManager()
        initialize_database(self.db_manager)

    def build_staged_faf_domestic(self) -> TableBuildResult:
        with self.db_manager.connection() as conn:
            conn.execute("DELETE FROM staged_faf_domestic")
            conn.execute(
                """
                INSERT INTO staged_faf_domestic
                SELECT
                    dms_orig,
                    dms_dest,
                    dms_mode,
                    sctg2,
                    trade_type,
                    dist_band,
                    tons_2017, tons_2018, tons_2019, tons_2020, tons_2021, tons_2022, tons_2023, tons_2024,
                    tons_2030, tons_2035, tons_2040, tons_2045, tons_2050,
                    value_2017, value_2018, value_2019, value_2020, value_2021, value_2022, value_2023, value_2024,
                    value_2030, value_2035, value_2040, value_2045, value_2050,
                    tmiles_2017, tmiles_2018, tmiles_2019, tmiles_2020, tmiles_2021, tmiles_2022, tmiles_2023, tmiles_2024,
                    tmiles_2030, tmiles_2035, tmiles_2040, tmiles_2045, tmiles_2050,
                    tons_2030_low, tons_2035_low, tons_2040_low, tons_2045_low, tons_2050_low,
                    tons_2030_high, tons_2035_high, tons_2040_high, tons_2045_high, tons_2050_high,
                    value_2030_low, value_2035_low, value_2040_low, value_2045_low, value_2050_low,
                    value_2030_high, value_2035_high, value_2040_high, value_2045_high, value_2050_high
                FROM raw_faf_hilo
                WHERE trade_type = 1
                """
            )
            row_count = conn.execute("SELECT COUNT(*) FROM staged_faf_domestic").fetchone()[0]
        return TableBuildResult("staged_faf_domestic", row_count)

    def build_staged_faf_historical_domestic(self) -> TableBuildResult:
        with self.db_manager.connection() as conn:
            conn.execute("DELETE FROM staged_faf_historical_domestic")
            conn.execute(
                """
                INSERT INTO staged_faf_historical_domestic
                SELECT
                    dms_origst,
                    dms_destst,
                    dms_mode,
                    sctg2,
                    trade_type,
                    tons_1997, tons_2002, tons_2007, tons_2012,
                    value_1997, value_2002, value_2007, value_2012,
                    tmiles_1997, tmiles_2002, tmiles_2007, tmiles_2012
                FROM raw_faf_historical
                WHERE trade_type = 1
                """
            )
            row_count = conn.execute("SELECT COUNT(*) FROM staged_faf_historical_domestic").fetchone()[0]
        return TableBuildResult("staged_faf_historical_domestic", row_count)

    def build_staged_fred_annual(self) -> TableBuildResult:
        with self.db_manager.connection() as conn:
            conn.execute("DELETE FROM staged_fred_annual")
            conn.execute(
                """
                INSERT INTO staged_fred_annual
                SELECT
                    EXTRACT(YEAR FROM date)::INTEGER AS year,
                    AVG(CASE WHEN series_id = 'GDPC1' THEN value END) AS gdp,
                    AVG(CASE WHEN series_id = 'GASDESW' THEN value END) AS diesel_price,
                    AVG(CASE WHEN series_id = 'INDPRO' THEN value END) AS industrial_production,
                    AVG(CASE WHEN series_id = 'UNRATE' THEN value END) AS unemployment_rate,
                    AVG(CASE WHEN series_id = 'PCU484484' THEN value END) AS ppi_transportation,
                    AVG(CASE WHEN series_id = 'TRFVOLUSM227NFWA' THEN value END) AS vehicle_miles
                FROM raw_fred_series
                GROUP BY 1
                ORDER BY 1
                """
            )
            row_count = conn.execute("SELECT COUNT(*) FROM staged_fred_annual").fetchone()[0]
        return TableBuildResult("staged_fred_annual", row_count)

    def build_staged_zone_to_state(self) -> TableBuildResult:
        with self.db_manager.connection() as conn:
            zone_frame = conn.execute(
                """
                SELECT
                    numeric_label AS zone_id,
                    short_description,
                    long_description
                FROM raw_faf_metadata_zones
                ORDER BY numeric_label
                """
            ).df()
            state_frame = conn.execute(
                """
                SELECT
                    numeric_label AS state_fips,
                    description AS state_name
                FROM raw_faf_metadata_states
                ORDER BY numeric_label
                """
            ).df()

            state_lookup = state_frame.set_index("state_name")["state_fips"].to_dict()
            mapping = zone_frame.copy()
            mapping["state_name"] = mapping.apply(
                lambda row: _infer_state_name(row["short_description"], row["long_description"]),
                axis=1,
            )
            mapping["state_fips"] = mapping["state_name"].map(state_lookup)
            mapping = mapping[
                ["zone_id", "short_description", "long_description", "state_fips", "state_name"]
            ].rename(
                columns={
                    "short_description": "zone_short_description",
                    "long_description": "zone_long_description",
                }
            )

            conn.execute("DELETE FROM staged_zone_to_state")
            conn.register("zone_state_mapping", mapping)
            conn.execute("INSERT INTO staged_zone_to_state SELECT * FROM zone_state_mapping")
            conn.unregister("zone_state_mapping")
            row_count = conn.execute("SELECT COUNT(*) FROM staged_zone_to_state").fetchone()[0]
        return TableBuildResult("staged_zone_to_state", row_count)

    def build_all(self) -> list[TableBuildResult]:
        return [
            self.build_staged_faf_domestic(),
            self.build_staged_faf_historical_domestic(),
            self.build_staged_fred_annual(),
            self.build_staged_zone_to_state(),
        ]
