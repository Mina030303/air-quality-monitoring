# Taiwan Air Quality Monitoring Platform

An end-to-end AQI monitoring project for Taiwan: automated data ingestion, validation, analytics, and a multi-page Streamlit dashboard.

## Highlights

- Reliable API ingestion from Taiwan MOENV datasets (`AQX_P_488`, `AQX_P_434`)
- Data validation with Pydantic v2 (schema checks, datetime parsing, coordinate validation)
- Hot + cold storage design
  - Hot: CSV for fast dashboard access
  - Cold: Neon PostgreSQL with pooled connections and UPSERT deduplication
- Analytics modules
  - Daily and monthly trends
  - County analysis and risk scoring
  - High-pollution hour ratio
  - Pollution spike detection
- Interactive Streamlit dashboard with Traditional Chinese/English toggle
- Scheduled ETL updates via GitHub Actions (commit only on data change)

## Repository Layout

- `app.py`: Streamlit entry page
- `main.py`: Data pipeline orchestration
- `src/`: ingestion, validation, storage, prediction, analysis utilities
- `pages/`: dashboard pages (trend, county analysis, risk, high-pollution hours, spikes)
- `data/`: raw, processed, and latest AQI files
- `output/`: generated tables and figures

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

3. Add environment variables in `.env`:

```env
API_KEY=your_moenv_api_key
# Optional:
# DATABASE_URL=your_neon_postgresql_url
```

4. Run pipeline (recommended before dashboard):

```bash
python main.py
```

5. Start dashboard:

```bash
streamlit run app.py
```

## Data Outputs

- Latest snapshots: `data/hourly_aqi.csv`, `data/daily_aqi.csv`
- Clean baselines: `data/processed/hourly_clean.csv`, `data/processed/daily_clean.csv`
- Analysis results: `output/tables/`, `output/figures/`

## Tech Stack

- Python, Pandas, Requests
- Streamlit, Altair, Matplotlib
- Pydantic v2, python-dotenv
- PostgreSQL (psycopg2)
- GitHub Actions


