from pathlib import Path

import pandas as pd

from src.ingestion.metadata_loader import MetadataLoader
from src.storage.raw_layer import RawLayerLoader
from src.storage.db_manager import DuckDBManager
from src.storage.schema_manager import initialize_database


def test_metadata_loader_reads_expected_sheets(tmp_path: Path):
    metadata_path = tmp_path / "metadata.xlsx"

    with pd.ExcelWriter(metadata_path, engine="openpyxl") as writer:
        pd.DataFrame({"Numeric Label": [1], "Description": ["Alabama"]}).to_excel(
            writer, sheet_name="State", index=False
        )
        pd.DataFrame(
            {
                "Numeric Label": [11],
                "Short Description": ["Birmingham AL"],
                "Long Description": ["Birmingham-Hoover-Talladega, AL CFS Area"],
            }
        ).to_excel(writer, sheet_name="FAF Zone (Domestic)", index=False)
        pd.DataFrame({"Numeric Label": [1], "Description": ["Live animals/fish"]}).to_excel(
            writer, sheet_name="Commodity (SCTG2)", index=False
        )
        pd.DataFrame({"Numeric Label": [1], "Description": ["Truck"]}).to_excel(
            writer, sheet_name="Mode", index=False
        )
        pd.DataFrame({"Numeric Label": [1], "Description": ["Domestic"]}).to_excel(
            writer, sheet_name="Trade Type", index=False
        )
        pd.DataFrame({"Numeric Label": [1], "Description": ["Below 100"]}).to_excel(
            writer, sheet_name="Distance Band", index=False
        )

    frames = MetadataLoader(metadata_path=metadata_path).load_all()

    assert set(frames) == {
        "raw_faf_metadata_states",
        "raw_faf_metadata_zones",
        "raw_faf_metadata_commodities",
        "raw_faf_metadata_modes",
        "raw_faf_metadata_trade_type",
        "raw_faf_metadata_distance_band",
    }
    assert frames["raw_faf_metadata_zones"]["short_description"].iloc[0] == "Birmingham AL"


def test_raw_layer_load_metadata_inserts_lookup_tables(monkeypatch, tmp_path: Path):
    db_path = tmp_path / "test.duckdb"
    manager = DuckDBManager(db_path=db_path)
    initialize_database(manager)

    sample_frames = {
        "raw_faf_metadata_states": pd.DataFrame({"numeric_label": [1], "description": ["Alabama"]}),
        "raw_faf_metadata_zones": pd.DataFrame(
            {
                "numeric_label": [11],
                "short_description": ["Birmingham AL"],
                "long_description": ["Birmingham-Hoover-Talladega, AL CFS Area"],
            }
        ),
        "raw_faf_metadata_commodities": pd.DataFrame(
            {"numeric_label": [1], "description": ["Live animals/fish"]}
        ),
        "raw_faf_metadata_modes": pd.DataFrame({"numeric_label": [1], "description": ["Truck"]}),
        "raw_faf_metadata_trade_type": pd.DataFrame(
            {"numeric_label": [1], "description": ["Domestic"]}
        ),
        "raw_faf_metadata_distance_band": pd.DataFrame(
            {"numeric_label": [1], "description": ["Below 100"]}
        ),
    }

    monkeypatch.setattr(MetadataLoader, "load_all", lambda self: sample_frames)

    validations = RawLayerLoader(db_manager=manager).load_metadata()

    assert all(validation.is_valid for validation in validations)
    states = manager.fetch_df("SELECT COUNT(*) AS count FROM raw_faf_metadata_states")["count"].iloc[0]
    assert states == 1
