# Taiwan Air Quality Monitoring Platform

A lightweight Streamlit dashboard for Taiwan air quality analytics.

## What It Does
- Fetches AQI data from Taiwan MOENV API
- Tracks hourly and daily patterns
- Shows trend, county-level risk, high-pollution hours, and spike detection
- Supports Traditional Chinese and English UI

## Data Sources and Files
- Realtime crawler outputs:
    - `data/hourly_aqi.csv`
    - `data/daily_aqi.csv`
- Historical baseline files:
    - `data/processed/hourly_clean.csv` (about 30 days)
    - `data/processed/daily_clean.csv` (about 2 years)

## Quick Start
1. Clone and enter the project:
```bash
git clone https://github.com/Mina030303/air-quality-monitoring.git
cd air_quality_monitoring_platform
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Add your API key:
```env
API_KEY=your_moenv_api_key
```

4. (Optional) Run local pipeline refresh:
```bash
python main.py
```

5. Start Streamlit:
```bash
streamlit run app.py
```

## Automation
GitHub Actions in `.github/workflows/main.yml` updates data on schedule:
- Hourly job updates `data/hourly_aqi.csv`
- Daily job updates `data/daily_aqi.csv`

## Main Pages
- `pages/trend.py`: Taiwan AQI trend
- `pages/county_analysis.py`: County pollution profile
- `pages/county_risk.py`: County risk scoring and spike summary
- `pages/high_pollution_hours.py`: High-pollution hour analysis
- `pages/spike_detection.py`: Spike event detection


