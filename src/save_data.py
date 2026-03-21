from pathlib import Path
import pandas as pd


def save_csv(df: pd.DataFrame, path):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(p, index=False, encoding="utf-8-sig")