from __future__ import annotations

import pandas as pd
import streamlit as st


def render_metrics_table(frame: pd.DataFrame) -> None:
    if frame.empty:
        st.info("No metrics available yet.")
        return
    st.dataframe(frame, use_container_width=True, hide_index=True)
