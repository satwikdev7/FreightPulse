from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go


def build_forecast_chart(
    actuals: pd.DataFrame,
    forecasts: pd.DataFrame,
    bts: pd.DataFrame,
    selected_models: list[str],
) -> go.Figure:
    figure = go.Figure()

    if not actuals.empty:
        figure.add_trace(
            go.Scatter(
                x=actuals["year"],
                y=actuals["total_tons"],
                mode="lines+markers",
                name="Actuals",
                line={"color": "#1f77b4", "width": 3},
            )
        )

    for model_name in selected_models:
        model_frame = forecasts[forecasts["model_name"] == model_name]
        if model_frame.empty:
            continue
        figure.add_trace(
            go.Scatter(
                x=model_frame["year"],
                y=model_frame["prediction"],
                mode="lines+markers",
                name=model_name,
            )
        )
        if {"lower_bound", "upper_bound"} <= set(model_frame.columns):
            figure.add_trace(
                go.Scatter(
                    x=list(model_frame["year"]) + list(model_frame["year"])[::-1],
                    y=list(model_frame["upper_bound"]) + list(model_frame["lower_bound"])[::-1],
                    fill="toself",
                    fillcolor="rgba(31, 119, 180, 0.08)",
                    line={"color": "rgba(255,255,255,0)"},
                    hoverinfo="skip",
                    showlegend=False,
                )
            )

    if not bts.empty:
        figure.add_trace(
            go.Scatter(
                x=bts["year"],
                y=bts["bts_tons_high"],
                mode="lines",
                line={"width": 0},
                showlegend=False,
                hoverinfo="skip",
            )
        )
        figure.add_trace(
            go.Scatter(
                x=bts["year"],
                y=bts["bts_tons_low"],
                mode="lines",
                fill="tonexty",
                fillcolor="rgba(255, 127, 14, 0.12)",
                line={"width": 0},
                name="BTS Band",
                hoverinfo="skip",
            )
        )
        figure.add_trace(
            go.Scatter(
                x=bts["year"],
                y=bts["bts_tons"],
                mode="lines+markers",
                name="BTS Forecast",
                line={"dash": "dash", "color": "#ff7f0e"},
            )
        )

    figure.update_layout(
        xaxis_title="Year",
        yaxis_title="Total Tons",
        hovermode="x unified",
        legend_title="Series",
        margin={"l": 20, "r": 20, "t": 20, "b": 20},
    )
    return figure
