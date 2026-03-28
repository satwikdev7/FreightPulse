# FreightPulse : Multi-Model Freight Demand Forecasting Engine

FreightPulse is an end-to-end freight demand forecasting engine built on US Bureau of Transportation Statistics Freight Analysis Framework (FAF) FAF5 shipment data, FRED macroeconomic indicators, DuckDB, Airflow, MLflow, and Streamlit. The project forecasts freight demand across 5 locked regional corridors and compares ARIMA, Prophet, and XGBoost outputs against both holdout actuals and BTS benchmark forecasts.

## Overview

FreightPulse was designed as a corridor-centric freight analytics project for U.S. transportation forecasting. The pipeline ingests raw FAF5 freight tables and FRED macroeconomic indicators, transforms them into model-ready annual corridor features, trains three different forecasting approaches, evaluates them against holdout actuals and BTS benchmark forecasts, and exposes the results through an analyst-facing Streamlit dashboard.

The project is built to show:

- End-to-end data engineering
- Reproducible forecasting workflows
- Multi-model comparison instead of single-model reporting
- Experiment tracking with MLflow
- Operational orchestration with Airflow-style DAGs

## Problem Statement

Freight planning is often constrained by static benchmark tables and manual spreadsheet workflows. FreightPulse addresses that by creating a reproducible pipeline that:

- ingests fresh shipment and economic data
- builds corridor-level time series
- engineers forecasting features
- compares classical statistical and ML models side by side
- benchmarks predictions against official BTS FAF projections

## Data Sources

## Stack

- Data: 
a. FAF5 HiLo regional dataset
b. FAF5 historical state dataset
c. FRED API
- Storage: DuckDB with `raw -> staged -> feature` layers
- Models: ARIMA, Prophet, XGBoost
- Tracking: MLflow
- App: Streamlit + Plotly
- Orchestration: Airflow DAG wrappers over `src/` modules

### Source Datasets

- `FAF5.7.1_HiLoForecasts.csv`
  Used for 2017–2024 actual regional flows and 2030–2050 BTS benchmark forecasts.
  Link : https://www.bts.gov/faf
- `FAF5.7.1_Reprocessed_1997-2012_State.csv`
  Used for historical state-level trend extension.
  Link : https://www.bts.gov/faf
- `FRED API`
  Used for economic indicators such as GDP, diesel prices, industrial production, unemployment, transportation PPI, and vehicle miles traveled.
  Link : https://fred.stlouisfed.org/docs/api/fred/

## Designed Corridors

The project is intentionally centered around 5 fixed freight corridors:

| Corridor | Origin FAF Zone | Destination FAF Zone | Why It Matters |
|---|---:|---:|---|
| LA_to_Chicago | 61 | 171 | Major long-haul and intermodal freight corridor |
| Houston_to_Atlanta | 481 | 131 | Energy and manufacturing flow into the Southeast |
| NYC_to_Miami | 362 | 121 | High-value East Coast consumer goods corridor |
| Dallas_to_Memphis | 482 | 471 | Logistics hub to logistics hub |
| Seattle_to_Portland | 531 | 411 | Pacific Northwest regional trade corridor |

## Architecture

```text
FAF5 + Historical FAF + FRED
        ↓
Raw DuckDB layer
        ↓
Staged DuckDB layer
        ↓
Feature store
        ↓
ARIMA / Prophet / XGBoost
        ↓
MLflow tracking
        ↓
Holdout + BTS benchmark evaluation
        ↓
Streamlit dashboard
```

### Modeling Strategy

- `ARIMA`
  Statistical baseline using annual corridor tonnage only.
- `Prophet`
  Trend-based model with selected economic regressors.
- `XGBoost`
  Pooled corridor model using lag, rolling, and macroeconomic features.

### Evaluation Strategy

- Holdout evaluation: train on `2017–2023`, test on `2024`
- Optional expanding-window evaluation support
- BTS benchmark comparison: compare model `2030` forecasts against BTS `2030` forecast bands
- Metrics: `MAPE`, `RMSE`, `MAE`

## Python Version

Use Python `3.11.x` in the project `.venv`.

## Setup

```bash
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Populate `.env` with:

- `FRED_API_KEY`
- `DUCKDB_PATH`
- `MLFLOW_TRACKING_URI`

On macOS, XGBoost also requires `libomp`:

```bash
brew install libomp
```

## Repository Layout

```text
FreightPulse/
├── Data/
│   ├── FAF5.7.1_HiLoForecasts/
│   ├── FAF5.7.1_Reprocessed_1997-2012_State/
│   ├── db/
│   └── processed/
├── Notebooks/
├── dags/
├── mlruns/
├── src/
└── tests/
```

## End-to-End Run Order

Initialize the database:

```bash
source .venv/bin/activate
python -m src.storage.schema_manager --init
```

Load raw FAF + metadata + FRED:

```bash
source .venv/bin/activate
python -c "from src.storage.raw_layer import RawLayerLoader; loader = RawLayerLoader(); loader.load_metadata(); loader.load_hilo(); loader.load_historical(); loader.load_fred()"
```

Build staged and feature tables:

```bash
source .venv/bin/activate
python -c "from src.storage.staged_layer import StagedLayerBuilder; from src.storage.feature_layer import FeatureLayerBuilder; StagedLayerBuilder().build_all(); FeatureLayerBuilder().build_all()"
```

Train models and log to MLflow:

```bash
source .venv/bin/activate
MPLCONFIGDIR=/tmp/.mpl python -c "from src.models.forecasting import ForecastingService; ForecastingService().train_all_models(log_to_mlflow=True)"
```

Run evaluation:

```bash
source .venv/bin/activate
MPLCONFIGDIR=/tmp/.mpl python -c "from src.evaluation.model_comparison import run_full_evaluation; run_full_evaluation()"
```

Launch the dashboard:

```bash
source .venv/bin/activate
streamlit run src/dashboard/app.py
```

## Pipeline Outputs

By the end of the run, the project produces:

- raw tables in DuckDB
- staged tables for domestic freight, historical freight, annual FRED, and zone-to-state mapping
- feature tables for annual corridor demand, enriched features, BTS forecasts, and historical corridor trends
- trained model artifacts for ARIMA, Prophet, and XGBoost
- forecast CSVs and evaluation tables
- MLflow runs for each `(corridor, model, training_strategy)` combination

## Dashboard

The Streamlit app includes:

- `Overview`
- `Forecast Detail`
- `Model Comparison`
- `BTS Benchmark`

It reads from:

- DuckDB feature tables
- processed forecast/evaluation CSVs
- MLflow run metadata

## Artifacts Produced

- Forecast CSVs: `Data/processed/forecasts/`
- Evaluation outputs: `Data/processed/evaluation/`
- Model pickles: `Data/processed/models/`
- MLflow tracking DB: `mlruns/mlflow.db`

## Sample Outputs


### Dashboard Screenshots

<img width="1862" height="989" alt="Screenshot 2026-03-27 at 22 29 26" src="https://github.com/user-attachments/assets/acff72ea-ffad-4072-aee3-5e51b5c2e91e" />
<img width="1879" height="992" alt="Screenshot 2026-03-27 at 22 29 19" src="https://github.com/user-attachments/assets/9a0cfd6b-977b-412f-9775-bb8f7f5ccccc" />
<img width="1889" height="982" alt="Screenshot 2026-03-27 at 22 29 10" src="https://github.com/user-attachments/assets/3e99de8a-5ab4-419d-874a-f3547c2da53b" />
<img width="1874" height="966" alt="Screenshot 2026-03-27 at 22 26 27" src="https://github.com/user-attachments/assets/dbf882ff-58e0-41ac-a53e-24017f0eaf83" />


## Tests

Run the full test suite:

```bash
source .venv/bin/activate
pytest
```

Current tests cover:

- loaders
- metadata parsing
- feature engineering helpers
- evaluation logic
- metrics
- model schema helpers
- basic ARIMA and Prophet smoke tests
- dashboard import and artifact compatibility checks

## Repository Highlights

- `src/storage/`
  DuckDB schema, raw ingestion, staged transforms, and feature-layer builders
- `src/models/`
  Forecasting logic, common forecast schema, and MLflow logging
- `src/evaluation/`
  Holdout scoring, BTS benchmark scoring, and model comparison
- `src/dashboard/`
  Analyst-facing Streamlit interface
- `dags/`
  Thin orchestration wrappers over the application code

## Airflow Note

The DAG files are thin wrappers that call `src/` functions directly. In the current local environment, the DAG modules intentionally fall back to `dag = None` if Airflow cannot initialize cleanly. To activate real DAG objects, update Airflow to use an absolute SQLite path instead of the current relative one.
