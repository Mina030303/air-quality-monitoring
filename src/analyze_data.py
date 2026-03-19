import pandas as pd

def missing_value_summary(df: pd.DataFrame) -> pd.Series:   # count missing values in each column
    return df.isna().sum()  # return missing count per column

def numeric_summary(df: pd.DataFrame) -> pd.DataFrame:   # generate a summary table for the dataset
    return df.describe(include="all")  # return descriptive statistics