"""
Analytics: forecasting, anomaly detection, correlation.
"""
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression


# ---------------------------------------------------------------------------
# Forecasting
# ---------------------------------------------------------------------------

def forecast(df: pd.DataFrame, periods: int = 8, method: str = "linear") -> pd.DataFrame:
    """
    Project future values for a time series.

    Parameters
    ----------
    df      : DataFrame with columns [time_period, value]
    periods : number of future periods to forecast
    method  : "linear" (sklearn) or "lstm" (Keras)

    Returns
    -------
    DataFrame with columns [time_period, value, lower, upper] for the forecast window.
    """
    series = df[["time_period", "value"]].dropna().copy()
    if len(series) < 4:
        return pd.DataFrame(columns=["time_period", "value", "lower", "upper"])

    if method == "lstm" and len(series) >= 50:
        return _lstm_forecast(series, periods)
    return _linear_forecast(series, periods)


def _linear_forecast(series: pd.DataFrame, periods: int) -> pd.DataFrame:
    series = series.copy()
    series["t"] = np.arange(len(series))

    X = series[["t"]].values
    y = series["value"].values

    model = LinearRegression().fit(X, y)

    # Residual std for confidence band
    residuals = y - model.predict(X)
    std = residuals.std()

    # Infer frequency
    freq = _infer_freq(series["time_period"])

    last_date = series["time_period"].iloc[-1]
    future_dates = pd.date_range(start=last_date, periods=periods + 1, freq=freq)[1:]
    future_t = np.arange(len(series), len(series) + periods).reshape(-1, 1)

    preds = model.predict(future_t)
    return pd.DataFrame({
        "time_period": future_dates,
        "value": preds,
        "lower": preds - 1.96 * std,
        "upper": preds + 1.96 * std,
    })


def _lstm_forecast(series: pd.DataFrame, periods: int) -> pd.DataFrame:
    # Import TF only when needed to avoid slow startup
    import tensorflow as tf
    from tensorflow.keras.models import Sequential
    from tensorflow.keras.layers import LSTM, Dense

    values = series["value"].values.astype(float)
    # Normalise
    v_min, v_max = values.min(), values.max()
    v_range = v_max - v_min if v_max != v_min else 1.0
    norm = (values - v_min) / v_range

    look_back = min(12, len(norm) // 3)
    X, y = [], []
    for i in range(len(norm) - look_back):
        X.append(norm[i: i + look_back])
        y.append(norm[i + look_back])
    X = np.array(X).reshape(-1, look_back, 1)
    y = np.array(y)

    model = Sequential([
        LSTM(50, input_shape=(look_back, 1)),
        Dense(1),
    ])
    model.compile(optimizer="adam", loss="mse")
    model.fit(X, y, epochs=50, batch_size=4, verbose=0)

    window = list(norm[-look_back:])
    preds_norm = []
    for _ in range(periods):
        inp = np.array(window[-look_back:]).reshape(1, look_back, 1)
        pred = float(model.predict(inp, verbose=0)[0][0])
        preds_norm.append(pred)
        window.append(pred)

    preds = np.array(preds_norm) * v_range + v_min
    std = (values - np.mean(values)).std()

    freq = _infer_freq(series["time_period"])
    last_date = series["time_period"].iloc[-1]
    future_dates = pd.date_range(start=last_date, periods=periods + 1, freq=freq)[1:]

    return pd.DataFrame({
        "time_period": future_dates,
        "value": preds,
        "lower": preds - 1.96 * std,
        "upper": preds + 1.96 * std,
    })


def _infer_freq(dates: pd.Series) -> str:
    if len(dates) < 2:
        return "QS"
    delta = (dates.iloc[-1] - dates.iloc[-2]).days
    if delta <= 35:
        return "MS"
    if delta <= 100:
        return "QS"
    if delta <= 200:
        return "2QS"
    return "YS"


# ---------------------------------------------------------------------------
# Anomaly detection
# ---------------------------------------------------------------------------

def detect_anomalies(df: pd.DataFrame, threshold: float = 2.5) -> pd.DataFrame:
    """
    Add an `is_anomaly` boolean column using Z-score method.
    """
    df = df.copy()
    values = df["value"].dropna()
    if len(values) < 4:
        df["is_anomaly"] = False
        return df
    mean = values.mean()
    std = values.std()
    if std == 0:
        df["is_anomaly"] = False
        return df
    df["z_score"] = (df["value"] - mean) / std
    df["is_anomaly"] = df["z_score"].abs() > threshold
    df.drop(columns=["z_score"], inplace=True)
    return df


# ---------------------------------------------------------------------------
# Correlation
# ---------------------------------------------------------------------------

def correlate(df1: pd.DataFrame, df2: pd.DataFrame) -> dict:
    """
    Compute Pearson and Spearman correlation between two time series aligned on time_period.

    Returns {"pearson": float, "spearman": float, "n_observations": int}
    """
    a = df1[["time_period", "value"]].dropna().set_index("time_period")
    b = df2[["time_period", "value"]].dropna().set_index("time_period")
    merged = a.join(b, how="inner", lsuffix="_a", rsuffix="_b").dropna()

    if len(merged) < 3:
        return {"pearson": None, "spearman": None, "n_observations": len(merged)}

    pearson = merged["value_a"].corr(merged["value_b"], method="pearson")
    spearman = merged["value_a"].corr(merged["value_b"], method="spearman")
    return {
        "pearson": round(float(pearson), 4),
        "spearman": round(float(spearman), 4),
        "n_observations": len(merged),
        "merged": merged.reset_index(),
    }
