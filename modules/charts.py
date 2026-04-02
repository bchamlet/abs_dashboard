"""
Plotly chart builders and export helpers.
"""
import io
import pandas as pd
import plotly.graph_objects as go


# ---------------------------------------------------------------------------
# Chart builders
# ---------------------------------------------------------------------------

def line_chart(
    df: pd.DataFrame,
    title: str,
    y_label: str = "Value",
    anomalies: pd.DataFrame | None = None,
) -> go.Figure:
    """
    Time series line chart.
    If anomalies DataFrame is supplied (must have is_anomaly column),
    anomalous points are overlaid as red markers.
    """
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["time_period"],
        y=df["value"],
        mode="lines",
        name=y_label,
        line=dict(width=2),
    ))

    if anomalies is not None and "is_anomaly" in anomalies.columns:
        flagged = anomalies[anomalies["is_anomaly"]]
        if not flagged.empty:
            fig.add_trace(go.Scatter(
                x=flagged["time_period"],
                y=flagged["value"],
                mode="markers",
                name="Anomaly",
                marker=dict(color="red", size=9, symbol="x"),
            ))

    fig.update_layout(
        title=title,
        xaxis_title="Period",
        yaxis_title=y_label,
        hovermode="x unified",
        template="plotly_white",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return fig


def forecast_chart(
    df: pd.DataFrame,
    forecast_df: pd.DataFrame,
    title: str,
    y_label: str = "Value",
) -> go.Figure:
    """
    Historical line + forecast line with 95% confidence band.
    """
    fig = go.Figure()

    # Historical
    fig.add_trace(go.Scatter(
        x=df["time_period"],
        y=df["value"],
        mode="lines",
        name="Historical",
        line=dict(width=2, color="#1f77b4"),
    ))

    # Confidence band (upper then lower in reverse for fill)
    fig.add_trace(go.Scatter(
        x=pd.concat([forecast_df["time_period"], forecast_df["time_period"][::-1]]),
        y=pd.concat([forecast_df["upper"], forecast_df["lower"][::-1]]),
        fill="toself",
        fillcolor="rgba(255,127,14,0.15)",
        line=dict(color="rgba(255,255,255,0)"),
        name="95% CI",
        showlegend=True,
    ))

    # Forecast line
    fig.add_trace(go.Scatter(
        x=forecast_df["time_period"],
        y=forecast_df["value"],
        mode="lines",
        name="Forecast",
        line=dict(width=2, color="#ff7f0e", dash="dash"),
    ))

    fig.update_layout(
        title=title,
        xaxis_title="Period",
        yaxis_title=y_label,
        hovermode="x unified",
        template="plotly_white",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return fig


def correlation_chart(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    labels: tuple[str, str],
    title: str = "Correlation",
) -> go.Figure:
    """
    Dual-axis line chart for two time series.
    """
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df1["time_period"],
        y=df1["value"],
        mode="lines",
        name=labels[0],
        yaxis="y1",
        line=dict(width=2, color="#1f77b4"),
    ))

    fig.add_trace(go.Scatter(
        x=df2["time_period"],
        y=df2["value"],
        mode="lines",
        name=labels[1],
        yaxis="y2",
        line=dict(width=2, color="#d62728"),
    ))

    fig.update_layout(
        title=title,
        xaxis=dict(title="Period"),
        yaxis=dict(title=labels[0], titlefont=dict(color="#1f77b4")),
        yaxis2=dict(
            title=labels[1],
            titlefont=dict(color="#d62728"),
            overlaying="y",
            side="right",
        ),
        hovermode="x unified",
        template="plotly_white",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return fig


# ---------------------------------------------------------------------------
# Export helpers
# ---------------------------------------------------------------------------

def to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")


def to_png_bytes(fig: go.Figure) -> bytes:
    return fig.to_image(format="png", width=1200, height=600, scale=2)
