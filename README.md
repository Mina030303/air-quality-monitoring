# Taiwan Air Quality Monitoring Platform

A production-style data application for Taiwan AQI monitoring, combining automated data ingestion, validation, analytics, and an interactive Streamlit dashboard.

## Key Features (and Special Technologies Used)

- Automated AQI ingestion from Taiwan MOENV open APIs
    - Uses resilient HTTP fetching with retry and exponential backoff to reduce transient API/network failures.

- Strong schema validation before storage and analysis
    - Uses Pydantic v2 models to normalize field names, parse multiple datetime formats, validate coordinates, and filter invalid records.

- Hot + cold data architecture
    - Hot layer: CSV files for fast dashboard rendering.
    - Cold layer: Neon PostgreSQL with connection pooling and UPSERT logic for long-term, deduplicated storage.

- Built-in analytical modules for decision support
    - Trend analysis (daily/monthly)
    - County-level profiling and risk scoring
    - High-pollution hour ratio analysis
    - Pollution spike detection by county/site/time

- Interactive dashboard with multilingual UX
    - Streamlit multi-page app with Traditional Chinese/English toggle.
    - Altair visualizations for clear trend and comparison charts.
    - Streamlit caching (`st.cache_data`) to improve load speed.

- Scheduled data refresh in CI/CD
    - GitHub Actions cron jobs update hourly/daily data files and commit only when data changes.

## MOENV Dataset Mapping

- Hourly history dataset: AQX_P_488
- Daily history dataset: AQX_P_434

## Project Structure

- App entry
    - `app.py`: Streamlit home page and navigation.

- Data pipeline
    - `main.py`: Pipeline orchestration (fetch, validate, save/sync, analysis output).
    - `src/crawler.py`: API fetching, deduplication, CSV merge/write.
    - `src/models.py`: Pydantic validation models.
    - `src/database.py`: PostgreSQL connection pool and batch upsert.

- Dashboard pages
    - `pages/trend.py`: National trend exploration.
    - `pages/county_analysis.py`: County comparison and profile analysis.
    - `pages/county_risk.py`: County risk scoring and ranking.
    - `pages/high_pollution_hours.py`: High-pollution hour behavior.
    - `pages/spike_detection.py`: Spike event exploration.

- Automation
    - `.github/workflows/main.yml`: Scheduled hourly/daily update jobs.

## Data Files

- Realtime data (crawler output)
    - `data/hourly_aqi.csv`
    - `data/daily_aqi.csv`

- Processed historical baseline
    - `data/processed/hourly_clean.csv` (~30 days)
    - `data/processed/daily_clean.csv` (~2 years)

- Analysis outputs
    - `output/tables/`: generated KPI and summary tables
    - `output/figures/`: generated visual assets

## Quick Start

1. Clone the repository:

```bash
git clone https://github.com/Mina030303/air-quality-monitoring.git
cd air_quality_monitoring_platform
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Create a `.env` file:

```env
API_KEY=your_moenv_api_key
# Optional (for cold storage):
# DATABASE_URL=your_neon_postgresql_url
```

4. Run data pipeline (optional but recommended before dashboard launch):

```bash
python main.py
```

5. Launch dashboard:

```bash
streamlit run app.py
```

## Deployment and Automation Notes

- Scheduled refresh is configured in `.github/workflows/main.yml`.
- The workflow commits data changes back to `main` only when files actually change.
- You can also trigger workflow updates manually with `workflow_dispatch`.

## Tech Stack

- Python, Pandas, Requests
- Streamlit, Altair, Matplotlib
- Pydantic v2, python-dotenv
- PostgreSQL (psycopg2) for persistent storage
- GitHub Actions for scheduled ETL automation


