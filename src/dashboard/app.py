from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

for parent in Path(__file__).resolve().parents:
    if (parent / "src").exists():
        parent_str = str(parent)
        if parent_str not in sys.path:
            sys.path.insert(0, parent_str)
        break

from src.dashboard.components.corridor_selector import render_corridor_selector
from src.dashboard.data_access import DashboardDataAccess
from src.dashboard.views import bts_benchmark, corridor_overview, forecast_detail, model_comparison


@st.cache_data(show_spinner=False)
def load_dashboard_payload() -> dict[str, object]:
    data = DashboardDataAccess()
    return {
        "annual": data.load_corridor_annual(),
        "forecasts": data.load_forecasts(),
        "comparison": data.load_model_comparison(),
        "bts_scores": data.load_bts_scores(),
        "bts": data.load_bts_forecast(),
        "feature_importance": data.load_feature_importance(),
        "mlflow_runs": data.load_mlflow_runs(),
    }


def render_app() -> None:
    st.set_page_config(page_title="FreightPulse", layout="wide")
    st.title("FreightPulse")
    st.caption("Multi-model freight demand forecasting across 5 U.S. corridors.")

    payload = load_dashboard_payload()
    annual = payload["annual"]
    forecasts = payload["forecasts"]
    comparison = payload["comparison"]
    bts_scores = payload["bts_scores"]
    bts = payload["bts"]
    feature_importance = payload["feature_importance"]
    mlflow_runs = payload["mlflow_runs"]

    if annual.empty:
        st.error("No corridor data found. Run ingestion, transform, training, and evaluation first.")
        return

    corridor_names = annual["corridor_name"].drop_duplicates().tolist()
    model_options = sorted(forecasts["model_name"].dropna().unique().tolist()) if not forecasts.empty else []

    if "applied_corridor" not in st.session_state:
        st.session_state.applied_corridor = corridor_names[0]
    if "applied_models" not in st.session_state:
        st.session_state.applied_models = model_options

    selected_page = st.sidebar.radio(
        "Page",
        ["Overview", "Forecast Detail", "Model Comparison", "BTS Benchmark"],
    )
    selected_corridor = render_corridor_selector(
        corridor_names,
        default_name=st.session_state.applied_corridor,
    )
    selected_models = st.sidebar.multiselect(
        "Models",
        model_options,
        default=st.session_state.applied_models,
    )

    if st.sidebar.button("Generate Forecast", type="primary", use_container_width=True):
        st.session_state.applied_corridor = selected_corridor
        st.session_state.applied_models = selected_models or model_options
        st.rerun()

    if st.sidebar.button("Reload Data", use_container_width=True):
        load_dashboard_payload.clear()
        st.rerun()

    with st.sidebar.expander("MLflow Runs", expanded=False):
        if mlflow_runs.empty:
            st.write("No MLflow runs found.")
        else:
            st.dataframe(mlflow_runs, use_container_width=True, hide_index=True)

    applied_corridor = st.session_state.applied_corridor
    applied_models = st.session_state.applied_models

    if selected_page == "Overview":
        corridor_overview.render_page(
            annual=annual,
            comparison=comparison,
            selected_corridor=applied_corridor,
            selected_models=applied_models,
        )
    elif selected_page == "Forecast Detail":
        forecast_detail.render_page(
            corridor_name=applied_corridor,
            annual=annual,
            forecasts=forecasts,
            bts=bts,
            comparison=comparison,
            selected_models=applied_models,
        )
    elif selected_page == "Model Comparison":
        model_comparison.render_page(
            comparison=comparison,
            feature_importance=feature_importance,
            selected_corridor=applied_corridor,
            selected_models=applied_models,
        )
    else:
        bts_benchmark.render_page(
            bts_scores=bts_scores,
            selected_corridor=applied_corridor,
            selected_models=applied_models,
        )


if __name__ == "__main__":
    render_app()
