from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

from src.utils.corridor_config import CorridorDefinition, LOCKED_CORRIDORS
from src.utils import paths

load_dotenv(paths.ROOT_DIR / ".env")


def _env_path(name: str, default: Path) -> Path:
    value = os.getenv(name)
    if not value:
        return default
    candidate = Path(value)
    return candidate if candidate.is_absolute() else paths.ROOT_DIR / candidate


@dataclass(frozen=True)
class AppConfig:
    project_root: Path
    data_dir: Path
    notebooks_dir: Path
    mlruns_dir: Path
    duckdb_path: Path
    faf_hilo_csv: Path
    faf_hilo_metadata_xlsx: Path
    faf_historical_csv: Path
    faf_historical_metadata_xlsx: Path
    fred_api_key: str | None
    mlflow_tracking_uri: str
    mlflow_experiment_name: str
    streamlit_port: int
    corridors: tuple[CorridorDefinition, ...]


def get_config() -> AppConfig:
    return AppConfig(
        project_root=paths.ROOT_DIR,
        data_dir=_env_path("DATA_DIR", paths.DATA_DIR),
        notebooks_dir=_env_path("NOTEBOOKS_DIR", paths.NOTEBOOKS_DIR),
        mlruns_dir=_env_path("MLRUNS_DIR", paths.MLRUNS_DIR),
        duckdb_path=_env_path("DUCKDB_PATH", paths.DUCKDB_PATH),
        faf_hilo_csv=_env_path("FAF_HILO_CSV", paths.FAF_HILO_CSV),
        faf_hilo_metadata_xlsx=_env_path("FAF_HILO_METADATA_XLSX", paths.FAF_HILO_METADATA_XLSX),
        faf_historical_csv=_env_path("FAF_HISTORICAL_CSV", paths.FAF_HISTORICAL_CSV),
        faf_historical_metadata_xlsx=_env_path(
            "FAF_HISTORICAL_METADATA_XLSX",
            paths.FAF_HISTORICAL_METADATA_XLSX,
        ),
        fred_api_key=os.getenv("FRED_API_KEY"),
        mlflow_tracking_uri=os.getenv("MLFLOW_TRACKING_URI", "sqlite:///mlruns/mlflow.db"),
        mlflow_experiment_name=os.getenv("MLFLOW_EXPERIMENT_NAME", "freightpulse"),
        streamlit_port=int(os.getenv("STREAMLIT_PORT", "8501")),
        corridors=LOCKED_CORRIDORS,
    )


CONFIG = get_config()
