# Taiwan Air Quality Monitoring Platform

An end-to-end air quality monitoring and forecasting project for Taiwan.
It combines automated data ingestion, validation, analytics, machine learning, and user-facing delivery through a dashboard and LINE bot.

## Key Features

- Automated AQI ingestion from Taiwan MOENV open data APIs
- Data cleaning and schema validation for stable downstream analysis
- Forecasting pipeline for short-term AQI prediction
- Multi-page Streamlit dashboard for trend, county, risk, and spike analysis
- LINE bot integration for county subscription, AQI lookup, and forecast queries
- Scheduled ETL and updates with GitHub Actions

## Project Structure

- `main.py`: pipeline orchestration entry point
- `app.py`: Streamlit dashboard entry point
- `src/`: data ingestion, cleaning, storage, prediction, and LINE bot services
- `pages/`: dashboard page modules
- `data/`: raw, processed, and latest AQI datasets
- `models/`: trained model artifacts
- `output/`: generated analysis tables and figures

## Tech Stack (Brief)

- Core: Python, Pandas, Requests
- Visualization and app: Streamlit, Altair, Matplotlib
- ML: scikit-learn, XGBoost, LightGBM, Joblib
- Backend and integration: Flask, line-bot-sdk, SQLAlchemy, PostgreSQL (psycopg2)
- Data quality and config: Pydantic v2, python-dotenv, Tenacity
- Automation: GitHub Actions

## Quick Start

1. Clone this repository.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Create a `.env` file:

```env
API_KEY=your_moenv_api_key
LINE_CHANNEL_ACCESS_TOKEN=your_line_token
LINE_CHANNEL_SECRET=your_line_secret
DATABASE_URL=your_postgresql_url
```

4. Run the pipeline:

```bash
python main.py
```

5. Launch the dashboard:

```bash
streamlit run app.py
```

## Output Artifacts

- Latest AQI snapshots in `data/`
- Processed datasets in `data/processed/`
- Analytical tables and figures in `output/`


