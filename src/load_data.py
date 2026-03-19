from pathlib import Path
import pandas as pd

RAW_DIR = Path("data/raw")  # path to raw data folder

def load_csv(filename: str) -> pd.DataFrame:  # load a CSV file from the raw data folder
    file_path = RAW_DIR / filename  # build full file path
    df = pd.read_csv(file_path)      # read CSV into DataFrame
    return df