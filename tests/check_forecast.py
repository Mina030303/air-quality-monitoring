from __future__ import annotations

from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
FORECAST_PATH = BASE_DIR / "data" / "forecast.csv"
REASONABLE_MIN = 20
REASONABLE_MAX = 150


def main() -> None:
    if not FORECAST_PATH.exists():
        raise FileNotFoundError(f"Forecast file not found: {FORECAST_PATH}")

    df = pd.read_csv(FORECAST_PATH)

    print("=" * 70)
    print("Forecast Preview (Top 5 Rows)")
    print("=" * 70)
    print(df.head(5).to_string(index=False))

    if "predicted_aqi" not in df.columns:
        raise ValueError("Column 'predicted_aqi' not found in forecast.csv")

    predicted = pd.to_numeric(df["predicted_aqi"], errors="coerce")

    negative_count = int((predicted < 0).sum())
    print("\nNegative AQI Check")
    print("=" * 70)
    if negative_count > 0:
        print(f"[FAIL] Found {negative_count} negative predicted AQI values.")
    else:
        print("[PASS] No negative predicted AQI values found.")

    avg_aqi = float(predicted.mean())
    print("\nAverage Predicted AQI Check")
    print("=" * 70)
    print(f"Average predicted AQI across all counties: {avg_aqi:.2f}")

    if REASONABLE_MIN <= avg_aqi <= REASONABLE_MAX:
        print(f"[PASS] Average AQI is within reasonable range ({REASONABLE_MIN}-{REASONABLE_MAX}).")
    else:
        print(f"[WARN] Average AQI is outside reasonable range ({REASONABLE_MIN}-{REASONABLE_MAX}).")


if __name__ == "__main__":
    main()
