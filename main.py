from src.load_data import load_csv
from src.clean_data import basic_cleaning
from src.analyze_data import missing_value_summary

def main():  
    df = load_csv("air_quality_raw.csv") # Load raw data
    clean_df = basic_cleaning(df)  # Clean data
    # Print basic information
    print("欄位數 / Number of columns:", len(clean_df.columns))
    print("缺失值摘要 / Missing value summary:")
    print(missing_value_summary(clean_df))

    # Save cleaned dataset
    clean_df.to_csv("data/processed/air_quality_cleaned.csv", index=False)


    print("資料筆數 / Number of rows:", len(clean_df))  
    
if __name__ == "__main__":
    main()
    # Load raw data
    df = load_csv("air_quality_raw.csv")
    # Clean data
    clean_df = basic_cleaning(df)
    # Print basic information
    print("資料筆數:", len(clean_df))
    print("欄位數:", len(clean_df.columns))
    print("缺失值摘要:")
    print(missing_value_summary(clean_df))
    # Save cleaned dataset
    clean_df.to_csv("data/processed/air_quality_cleaned.csv", index=False)


if __name__ == "__main__":
    main()