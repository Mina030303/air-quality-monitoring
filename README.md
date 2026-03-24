# Taiwan Air Quality Monitoring & Analytics Platform

## Project Goal
This project is an end-to-end data pipeline and interactive dashboard for monitoring Taiwan's air quality. It automatically ingests data from the MOENV Open Data API, processes and cleans the dataset, runs statistical anomaly detections, and serves the results on a multilingual Streamlit web dashboard. The objective is to provide a clean, accessible interface for analyzing pollution trends, identifying high-risk counties, and detecting sudden pollution spikes.

## Key Features
- **Automated Data Pipeline:** Scheduled fetching and cleaning of hourly & daily air quality data (AQI, PM2.5, PM10, O3).
- **Trend Analysis:** 7-day rolling average tracking to differentiate short-term spikes from sustained pollution events.
- **County-Level Insights:** Multi-dimensional ranking of counties based on mean AQI, atmospheric volatility (standard deviation), and high-pollution ratios.
- **Spike Detection:** Built-in statistical models (`Rolling Mean Threshold` and `Z-Score` anomaly detection) to identify short-term extreme environmental events.
- **Interactive Multilingual Dashboard:** A responsive UI built with Streamlit and Altair, supporting full runtime toggling between Traditional Chinese and English.

## Tech Stack
- **Data Ingestion & Processing:** Python, Pandas, Requests
- **Data Visualization & Frontend:** Streamlit, Altair, Matplotlib
- **Environment Management:** python-dotenv
- **Prototyping:** Jupyter Notebook

## Project Structure
```text
air_quality_monitoring_platform/
├── app.py                     # Main entry point for the Streamlit dashboard
├── main.py                    # ETL pipeline script (Fetch -> Clean -> Analyze -> Export)
├── bootstrap_data.py          # Initial historical data loading utility
├── config.py                  # Multilingual UI dictionary registry (zh/en)
├── utils.py                   # Global style definitions, CSS overrides, and UI helper functions
├── requirements.txt           # Python dependencies
├── data/                      
│   ├── raw/                   # Raw CSV dumps directly from the API
│   └── processed/             # Cleaned, standardized, and typed datasets
├── output/                    
│   ├── figures/               # Static exported charts
│   └── tables/                # Aggregated analytical results (trends, ratio, spike summaries)
├── pages/                     # Streamlit frontend analysis views
│   ├── trend.py                   # Taiwan overall AQI trend chart
│   ├── county_analysis.py         # County-level pollution statistics
│   ├── high_pollution_hours.py    # Risk assessment by hour-of-the-day 
│   └── spike_detection.py         # Anomaly tracking using Rolling means/Z-scores
└── src/                       # Backend logic modules
    ├── fetch_data.py              # MOENV API request handlers
    ├── clean_data.py              # Data cleaning, typing, and missing-value handling
    ├── analyze_data.py            # Core statistical functions and spike detection algorithms
    ├── save_data.py               # File I/O utilities
    └── update_data.py             # Pipeline orchestrators
```

## Setup & Local Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/Mina030303/air-quality-monitoring.git
   cd air_quality_monitoring_platform
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure Environment Variables:**
   Create a `.env` file in the root directory and add your MOENV API key:
   ```env
   API_KEY=your_moenv_api_key_here
   ```

4. **Run the Data Pipeline (Optional):**
   If you wish to fetch the latest data from the API and regenerate analytical tables:
   ```bash
   python main.py
   ```
   *(Note: For read-only environments, the `data/processed/` and `output/tables/` folders already contain cached data.)*

5. **Launch the Dashboard:**
   ```bash
   streamlit run app.py
   ```

## Cloud Deployment (Streamlit Community Cloud)
This project is structured with a hard decoupling of backend (ETL) and frontend (Streamlit) logic. To deploy:
1. Ensure your `.gitignore` does **not** exclude `data/processed/` or `output/tables/` so the cloud app has data to display.
2. Connect your GitHub repository to Streamlit Community Cloud.
3. Set the Main file path to `app.py`.
4. Launch the application (The deployed app runs in a read-only mode using the precomputed CSV files stored in the repository.).


