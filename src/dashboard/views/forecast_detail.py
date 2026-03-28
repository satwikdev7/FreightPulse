from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

for parent in Path(__file__).resolve().parents:
    if (parent / "src").exists():
        parent_str = str(parent)
        if parent_str not in sys.path:
            sys.path.insert(0, parent_str)
        break

from src.dashboard.components.forecast_chart import build_forecast_chart
from src.dashboard.components.metrics_table import render_metrics_table


def render_page(
    corridor_name: str,
    annual: pd.DataFrame,
    forecasts: pd.DataFrame,
    bts: pd.DataFrame,
    comparison: pd.DataFrame,
    selected_models: list[str],
) -> None:
    st.subheader(f"Forecast Detail: {corridor_name}")
    corridor_actuals = annual[annual["corridor_name"] == corridor_name]
    corridor_forecasts = forecasts[forecasts["corridor_name"] == corridor_name]
    if selected_models:
        corridor_forecasts = corridor_forecasts[corridor_forecasts["model_name"].isin(selected_models)]
    corridor_bts = bts[bts["corridor_name"] == corridor_name]
    figure = build_forecast_chart(corridor_actuals, corridor_forecasts, corridor_bts, selected_models)
    st.plotly_chart(figure, use_container_width=True)

    st.markdown("**Model Metrics**")
    filtered_comparison = comparison[comparison["corridor_name"] == corridor_name]
    if selected_models:
        filtered_comparison = filtered_comparison[filtered_comparison["model_name"].isin(selected_models)]
    render_metrics_table(filtered_comparison.reset_index(drop=True))

    st.markdown("**Forecast Rows**")
    render_metrics_table(
        corridor_forecasts.sort_values(["model_name", "forecast_type", "year"]).reset_index(drop=True)
    )
