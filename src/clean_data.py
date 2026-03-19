import pandas as pd

def basic_cleaning(df: pd.DataFrame) -> pd.DataFrame:   #perform basic data cleaning
    df = df.copy()  # copy to avoid modifying original data
    df.columns = [col.strip() for col in df.columns]  # clean column names (remove whitespace)
    df = df.replace(["-", "", "NA", "N/A"], pd.NA)  # replace common missing-value strings with pandas NA
