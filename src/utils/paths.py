from __future__ import annotations

from pathlib import Path


def project_root() -> Path:
    """Return the repository root based on this file location."""
    return Path(__file__).resolve().parents[2]


ROOT_DIR = project_root()
DATA_DIR = ROOT_DIR / "Data"
NOTEBOOKS_DIR = ROOT_DIR / "Notebooks"
MLRUNS_DIR = ROOT_DIR / "mlruns"

FAF_HILO_DIR = DATA_DIR / "FAF5.7.1_HiLoForecasts"
FAF_HISTORICAL_DIR = DATA_DIR / "FAF5.7.1_Reprocessed_1997-2012_State"
DB_DIR = DATA_DIR / "db"
PROCESSED_DIR = DATA_DIR / "processed"
PROCESSED_EXPORTS_DIR = PROCESSED_DIR / "exports"
PROCESSED_FORECASTS_DIR = PROCESSED_DIR / "forecasts"
PROCESSED_EVALUATION_DIR = PROCESSED_DIR / "evaluation"

FAF_HILO_CSV = FAF_HILO_DIR / "FAF5.7.1_HiLoForecasts.csv"
FAF_HILO_METADATA_XLSX = FAF_HILO_DIR / "FAF5_metadata.xlsx"
FAF_HISTORICAL_CSV = FAF_HISTORICAL_DIR / "FAF5.7.1_Reprocessed_1997-2012_State.csv"
FAF_HISTORICAL_METADATA_XLSX = FAF_HISTORICAL_DIR / "FAF5_metadata.xlsx"
DUCKDB_PATH = DB_DIR / "freightpulse.duckdb"


def ensure_project_directories() -> None:
    """Create the writable project directories used by the pipeline."""
    for path in (
        DB_DIR,
        PROCESSED_DIR,
        PROCESSED_EXPORTS_DIR,
        PROCESSED_FORECASTS_DIR,
        PROCESSED_EVALUATION_DIR,
        MLRUNS_DIR,
    ):
        path.mkdir(parents=True, exist_ok=True)
