# Taiwan Air Quality Dashboard

## Overview
This project helps you monitor air quality in Taiwan.
It has two parts:
- A data pipeline to fetch, clean, and summarize data
- A Streamlit dashboard to view charts and insights

The dashboard supports Traditional Chinese and English.

## Main Features
- Daily and hourly air quality analysis (AQI, PM2.5, PM10, O3)
- Trend charts with 7-day moving average
- County risk ranking
- High pollution hour analysis
- Spike detection for unusual pollution events

## Tech Stack
- Python
- Pandas
- Streamlit
- Altair
- python-dotenv

## Project Structure
```text
air_quality_monitoring_platform/
├── app.py
├── main.py
├── bootstrap_data.py
├── config.py
├── utils.py
├── requirements.txt
├── pages/
│   ├── trend.py
│   ├── county_analysis.py
│   ├── county_risk.py
│   ├── high_pollution_hours.py
│   └── spike_detection.py
├── src/
│   ├── fetch_data.py
│   ├── clean_data.py
│   ├── analyze_data.py
│   ├── save_data.py
│   └── update_data.py
├── data/
│   ├── raw/
│   └── processed/
└── output/
    ├── figures/
    └── tables/
```

## Quick Start

1. Clone the repo
```bash
git clone https://github.com/Mina030303/air-quality-monitoring.git
cd air_quality_monitoring_platform
```

2. Install packages
```bash
pip install -r requirements.txt
```

3. Create `.env`
```env
API_KEY=your_moenv_api_key_here
```

4. (Optional) Run data pipeline
```bash
python main.py
```

5. Run dashboard
```bash
streamlit run app.py
```

## Notes
- If you do not run `main.py`, the app can still use existing CSV files in `data/processed/` and `output/tables/`.
- For Streamlit Cloud, keep `data/processed/` and `output/tables/` in the repo so charts can load.


