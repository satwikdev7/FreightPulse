from __future__ import annotations

import streamlit as st


def render_corridor_selector(corridor_names: list[str], default_name: str | None = None) -> str:
    default_index = 0
    if default_name and default_name in corridor_names:
        default_index = corridor_names.index(default_name)
    return st.sidebar.selectbox("Corridor", corridor_names, index=default_index)
