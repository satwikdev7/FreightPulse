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


def render_page(
    annual: pd.DataFrame,
    comparison: pd.DataFrame,
    selected_corridor: str,
    selected_models: list[str],
) -> None:
    st.subheader(f"Corridor Overview: {selected_corridor}")
    if annual.empty:
        st.warning("Annual corridor data is not available yet.")
        return

    corridor_annual = annual[annual["corridor_name"] == selected_corridor].copy()
    latest_year = int(corridor_annual["year"].max())
    latest = corridor_annual[corridor_annual["year"] == latest_year].copy()
    previous = corridor_annual[corridor_annual["year"] == latest_year - 1][["corridor_id", "total_tons"]].rename(
        columns={"total_tons": "previous_tons"}
    )
    latest = latest.merge(previous, on="corridor_id", how="left")
    latest["yoy_pct"] = ((latest["total_tons"] - latest["previous_tons"]) / latest["previous_tons"]) * 100

    corridor_comparison = comparison[comparison["corridor_name"] == selected_corridor].copy()
    if selected_models:
        corridor_comparison = corridor_comparison[corridor_comparison["model_name"].isin(selected_models)]

    metric_columns = st.columns(3)
    latest_row = latest.iloc[0]
    metric_columns[0].metric("Latest Tons", f"{latest_row.total_tons:,.1f}", delta=f"{latest_row.yoy_pct:.2f}%")
    metric_columns[1].metric("Latest Value", f"{latest_row.total_value:,.1f}")
    metric_columns[2].metric("Latest Ton-Miles", f"{latest_row.total_tmiles:,.1f}")

    if not corridor_comparison.empty:
        best_model = corridor_comparison.sort_values("mape").iloc[0]
        st.caption(f"Best selected model: {best_model['model_name']} | MAPE {best_model['mape']:.2f}%")

    trend = px.line(
        corridor_annual,
        x="year",
        y="total_tons",
        markers=True,
        title=f"2017–2024 Freight Volume Trend: {selected_corridor}",
    )
    st.plotly_chart(trend, use_container_width=True)

    if not corridor_comparison.empty:
        st.markdown("**Selected Model Summary**")
        st.dataframe(
            corridor_comparison.sort_values("mape").reset_index(drop=True),
            use_container_width=True,
            hide_index=True,
        )
