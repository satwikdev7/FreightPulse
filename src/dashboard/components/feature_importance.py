from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st


def render_feature_importance(frame: pd.DataFrame, top_n: int = 10) -> None:
    if frame.empty:
        st.info("No XGBoost feature importance artifact found yet.")
        return
    top = frame.sort_values("importance", ascending=False).head(top_n)
    figure = px.bar(
        top,
        x="importance",
        y="feature",
        orientation="h",
        title=f"Top {top_n} XGBoost Features",
    )
    figure.update_layout(yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(figure, use_container_width=True)
