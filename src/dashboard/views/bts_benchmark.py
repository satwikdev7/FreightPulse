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

from src.dashboard.components.metrics_table import render_metrics_table


def render_page(
    bts_scores: pd.DataFrame,
    selected_corridor: str | None = None,
    selected_models: list[str] | None = None,
) -> None:
    st.subheader("BTS Benchmark")
    if bts_scores.empty:
        st.warning("BTS benchmark scores are not available yet.")
        return

    frame = bts_scores.copy()
    if selected_corridor:
        frame = frame[frame["corridor_name"] == selected_corridor]
    if selected_models:
        frame = frame[frame["model_name"].isin(selected_models)]
    if frame.empty:
        st.info("No BTS benchmark rows match the selected corridor and model filters.")
        return

    deviation_chart = px.bar(
        frame,
        x="corridor_name",
        y="pct_deviation_vs_bts",
        color="model_name",
        barmode="group",
        title="2030 Deviation vs BTS Forecast",
    )
    st.plotly_chart(deviation_chart, use_container_width=True)
    render_metrics_table(frame)
