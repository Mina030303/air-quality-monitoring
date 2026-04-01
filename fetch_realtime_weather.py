import os
import requests
import pandas as pd
import logging
from sqlalchemy import create_engine, Column, String, Float, DateTime, Integer, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# SQLAlchemy setup
Base = declarative_base()

class WeatherObs(Base):
    __tablename__ = 'weather_obs'
    id = Column(Integer, primary_key=True, autoincrement=True)
    StationName = Column(String, nullable=False)
    CountyName = Column(String, nullable=True)
    ObsTime = Column(DateTime, nullable=False)
    AirTemperature = Column(Float, nullable=True)
    WindSpeed = Column(Float, nullable=True)
    WindDirection = Column(Float, nullable=True)
    Precipitation = Column(Float, nullable=True)
    __table_args__ = (UniqueConstraint('StationName', 'ObsTime', name='_station_obs_uc'),)


def clean_value(val, nulls=(-99, -999)):
    try:
        if val in nulls:
            return None
        return float(val)
    except Exception:
        return None

def fetch_weather_data(api_key):
    url = f'https://opendata.cwa.gov.tw/api/v1/rest/datastore/O-A0003-001?Authorization={api_key}'
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data
    except Exception as e:
        logging.error(f"Failed to fetch data: {e}")
        return None

def parse_weather_data(data):
    records = []
    try:
        stations = data['records']['Station']
        for s in stations:
            try:
                station_name = s.get('StationName')
                county_name = s.get('CountyName')
                obs_time = s.get('ObsTime')
                obs_time = datetime.strptime(obs_time, '%Y-%m-%d %H:%M:%S') if obs_time else None
                elements = {e['ElementName']: e['ElementValue'] for e in s.get('WeatherElement', [])}
                air_temp = clean_value(elements.get('AirTemperature'))
                wind_speed = clean_value(elements.get('WindSpeed'))
                wind_dir = clean_value(elements.get('WindDirection'))
                precip = clean_value(elements.get('Precipitation'), nulls=(-99, -999, -998))
                records.append({
                    'StationName': station_name,
                    'CountyName': county_name,
                    'ObsTime': obs_time,
                    'AirTemperature': air_temp,
                    'WindSpeed': wind_speed,
                    'WindDirection': wind_dir,
                    'Precipitation': precip
                })
            except Exception as e:
                logging.warning(f"Error parsing station data: {e}")
    except Exception as e:
        logging.error(f"Error parsing weather data: {e}")
    return records

def upsert_weather_data(records, db_url):
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()
    Base.metadata.create_all(engine)
    for rec in records:
        stmt = insert(WeatherObs).values(**rec).on_conflict_do_nothing(index_elements=['StationName', 'ObsTime'])
        try:
            session.execute(stmt)
        except Exception as e:
            logging.error(f"Upsert error: {e}")
    session.commit()
    session.close()

def main():
    api_key = os.getenv('CWA_API_KEY') or 'CWA_API_KEY'  # Replace with your actual key or set env var
    db_url = os.getenv('DATABASE_URL') or 'postgresql://user:password@localhost:5432/yourdb'  # Set your DB URL
    logging.info("Fetching weather data from CWA API...")
    data = fetch_weather_data(api_key)
    if not data:
        logging.error("No data fetched. Exiting.")
        return
    records = parse_weather_data(data)
    if not records:
        logging.error("No records parsed. Exiting.")
        return
    df = pd.DataFrame(records)
    logging.info(f"Parsed {len(df)} records. Upserting to database...")
    upsert_weather_data(records, db_url)
    logging.info("Weather data upsert complete.")

if __name__ == '__main__':
    main()