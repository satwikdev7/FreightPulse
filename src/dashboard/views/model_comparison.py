from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

for parent in Path(__file__).resolve().parents:
    if (parent / "src").exists():
        parent_str = str(parent)
        if parent_str not in sys.path:
            sys.path.insert(0, parent_str)
        break

from src.dashboard.components.feature_importance import render_feature_importance
from src.dashboard.components.metrics_table import render_metrics_table


def render_page(
    comparison: pd.DataFrame,
    feature_importance: pd.DataFrame,
    selected_corridor: str,
    selected_models: list[str],
) -> None:
    st.subheader("Model Comparison")
    if comparison.empty:
        st.warning("Comparison data is not available yet.")
        return

    frame = comparison.copy()
    if selected_corridor:
        frame = frame[frame["corridor_name"] == selected_corridor]
    if selected_models:
        frame = frame[frame["model_name"].isin(selected_models)]
    if frame.empty:
        st.info("No comparison rows match the selected corridor and model filters.")
        return

    mape_chart = px.bar(
        frame,
        x="corridor_name",
        y="mape",
        color="model_name",
        barmode="group",
        title="MAPE by Corridor and Model",
    )
    st.plotly_chart(mape_chart, use_container_width=True)

    rmse_heatmap = px.density_heatmap(
        frame,
        x="model_name",
        y="corridor_name",
        z="rmse",
        histfunc="avg",
        color_continuous_scale="Blues",
        title="RMSE Heatmap",
    )
    st.plotly_chart(rmse_heatmap, use_container_width=True)

    render_metrics_table(frame.reset_index(drop=True))
    render_feature_importance(feature_importance)
