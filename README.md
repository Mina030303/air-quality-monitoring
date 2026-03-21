# Air Quality Monitoring and Data Platform

## Project Goal
This project builds a small data pipeline for air quality data using an API-based ingestion process. It automatically collects, cleans, and analyzes pollution data to support trend monitoring and basic insights.

## Tech Stack
- Python
- pandas
- Jupyter Notebook
- Matplotlib
- REST API (MOENV Open Data)

## Project Structure
- `data/raw/`: raw data (optional fallback)
- `data/processed/`: cleaned data
- `notebooks/`: data exploration and validation
- `src/`: reusable pipeline scripts
- `output/`: analysis results (tables, figures)

## Current Stage
MVP: API ingestion → data cleaning → aggregation → basic analysis

---

## Example Analysis: Average AQI by County

This analysis computes the average AQI for each county and ranks regions by pollution level.

Top results:

- 南投縣: 89  
- 臺南市: 62  
- 屏東縣: 53  
- 新北市: 41

The results are automatically generated from the API-based pipeline and saved as structured output files.
