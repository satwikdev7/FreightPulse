# FreightPulse

FreightPulse is an end-to-end freight demand forecasting engine built on FAF5 shipment data, FRED macroeconomic indicators, DuckDB, Airflow, MLflow, and Streamlit. The project forecasts freight demand across 5 locked regional corridors and compares ARIMA, Prophet, and XGBoost outputs against both holdout actuals and BTS benchmark forecasts.

## Stack

- Data: FAF5 HiLo regional data, FAF5 historical state data, FRED API
- Storage: DuckDB with `raw -> staged -> feature` layers
- Models: ARIMA, Prophet, XGBoost
- Tracking: MLflow
- App: Streamlit + Plotly
- Orchestration: Airflow DAG wrappers over `src/` modules

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

## Airflow Note

The DAG files are thin wrappers that call `src/` functions directly. In the current local environment, the DAG modules intentionally fall back to `dag = None` if Airflow cannot initialize cleanly. To activate real DAG objects, update Airflow to use an absolute SQLite path instead of the current relative one.
