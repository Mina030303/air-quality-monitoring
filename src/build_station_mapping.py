from __future__ import annotations

import io
import math
import os
from pathlib import Path
from typing import Any

import certifi
import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine
from urllib3.exceptions import InsecureRequestWarning

MOENV_URL = "https://data.moenv.gov.tw/api/v2/aqx_p_07"
CWA_URL = "https://opendata.cwa.gov.tw/api/v1/rest/datastore/O-A0001-001"
BASE_DIR = Path(__file__).resolve().parent.parent
_SSL_FALLBACK_USED = False


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
	"""Calculate great-circle distance (km) between two WGS84 points."""
	earth_radius_km = 6371.0
	phi1 = math.radians(lat1)
	phi2 = math.radians(lat2)
	delta_phi = math.radians(lat2 - lat1)
	delta_lambda = math.radians(lon2 - lon1)

	a = (
		math.sin(delta_phi / 2) ** 2
		+ math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
	)
	c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
	return earth_radius_km * c


def _to_float(value: Any) -> float | None:
	if value is None:
		return None
	try:
		return float(str(value).strip())
	except (TypeError, ValueError):
		return None


def _safe_json(response: requests.Response) -> Any:
	"""Parse JSON safely and raise a clear error message on failure."""
	try:
		return response.json()
	except ValueError as exc:
		preview = response.text[:300].replace("\n", " ")
		raise ValueError(
			f"Non-JSON API response (status={response.status_code}). "
			f"Body preview: {preview!r}"
		) from exc


def _request_with_tls_fallback(url: str, params: dict[str, Any], timeout: int = 30) -> requests.Response:
	"""Try CA-verified TLS first; fallback to verify=False only if SSL validation fails."""
	global _SSL_FALLBACK_USED
	try:
		return requests.get(url, params=params, timeout=timeout, verify=certifi.where())
	except requests.exceptions.SSLError:
		if not _SSL_FALLBACK_USED:
			requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
			_SSL_FALLBACK_USED = True
		return requests.get(url, params=params, timeout=timeout, verify=False)


def _moenv_records_from_csv_text(text: str) -> list[dict[str, Any]]:
	"""Fallback parser when MOENV returns CSV/text instead of JSON."""
	stripped = text.lstrip("\ufeff").strip()
	if not stripped:
		return []

	df = pd.read_csv(io.StringIO(stripped), engine="python")
	if df.empty:
		return []

	df.columns = [str(col).strip().lower() for col in df.columns]
	return df.to_dict(orient="records")


def _fetch_moenv_air_stations(api_key: str | None = None, timeout: int = 30) -> list[dict[str, Any]]:
	params: dict[str, Any] = {"format": "json", "limit": 2000}
	if api_key:
		params["api_key"] = api_key

	response = _request_with_tls_fallback(MOENV_URL, params=params, timeout=timeout)
	response.raise_for_status()

	try:
		payload = _safe_json(response)
		if isinstance(payload, list):
			records = payload
		elif isinstance(payload, dict):
			records = payload.get("records", [])
			if isinstance(records, dict):
				records = records.get("records", [])
			if not isinstance(records, list):
				records = []
		else:
			records = []
	except ValueError:
		# Some environments/endpoints may return CSV even when format=json is requested.
		records = _moenv_records_from_csv_text(response.text)

	if not records:
		body = response.text.strip()
		if "api_key 不存在" in body or "api_key" in body.lower():
			raise ValueError("MOENV API returned no records due to missing/invalid API_KEY.")

	stations: list[dict[str, Any]] = []
	for record in records:
		lon = _to_float(record.get("twd97lon"))
		lat = _to_float(record.get("twd97lat"))

		if lon is None or lat is None:
			lon = _to_float(record.get("longitude"))
			lat = _to_float(record.get("latitude"))

		if lon is None or lat is None:
			continue

		site_id = record.get("siteid")
		site_name = record.get("sitename")
		if site_id is None or site_name is None:
			continue

		stations.append(
			{
				"siteid": str(site_id),
				"sitename": str(site_name),
				"latitude": lat,
				"longitude": lon,
			}
		)

	return stations


def _extract_wgs84_from_coordinates(coordinates: list[dict[str, Any]]) -> tuple[float | None, float | None]:
	for coord in coordinates:
		if str(coord.get("CoordinateName", "")).upper() == "WGS84":
			lat = _to_float(coord.get("StationLatitude") or coord.get("Coordinate2"))
			lon = _to_float(coord.get("StationLongitude") or coord.get("Coordinate1"))
			if lat is not None and lon is not None:
				return lat, lon

	for coord in coordinates:
		lat = _to_float(coord.get("StationLatitude") or coord.get("Coordinate2"))
		lon = _to_float(coord.get("StationLongitude") or coord.get("Coordinate1"))
		if lat is not None and lon is not None:
			return lat, lon

	return None, None


def _fetch_cwa_weather_stations(api_key: str, timeout: int = 30) -> list[dict[str, Any]]:
	params = {"Authorization": api_key, "format": "JSON"}
	response = _request_with_tls_fallback(CWA_URL, params=params, timeout=timeout)
	response.raise_for_status()
	payload = _safe_json(response)

	station_records = payload.get("records", {}).get("Station", [])
	if not isinstance(station_records, list):
		station_records = []

	stations: list[dict[str, Any]] = []
	for station in station_records:
		station_id = station.get("StationId")
		station_name = station.get("StationName")
		coords = station.get("GeoInfo", {}).get("Coordinates", [])
		if not isinstance(coords, list):
			coords = []

		lat, lon = _extract_wgs84_from_coordinates(coords)
		if station_id is None or station_name is None or lat is None or lon is None:
			continue

		stations.append(
			{
				"StationId": str(station_id),
				"StationName": str(station_name),
				"StationLatitude": lat,
				"StationLongitude": lon,
			}
		)

	return stations


def build_station_weather_mapping(cwa_api_key: str, moenv_api_key: str | None = None) -> pd.DataFrame:
	epa_stations = _fetch_moenv_air_stations(api_key=moenv_api_key)
	cwa_stations = _fetch_cwa_weather_stations(api_key=cwa_api_key)

	if not epa_stations:
		raise ValueError("No EPA station data available from MOENV API.")
	if not cwa_stations:
		raise ValueError("No weather station data available from CWA API.")

	results: list[dict[str, Any]] = []

	for epa in epa_stations:
		nearest_station: dict[str, Any] | None = None
		nearest_distance = float("inf")

		for cwa in cwa_stations:
			distance = haversine_distance(
				epa["latitude"],
				epa["longitude"],
				cwa["StationLatitude"],
				cwa["StationLongitude"],
			)
			if distance < nearest_distance:
				nearest_distance = distance
				nearest_station = cwa

		if nearest_station is None:
			continue

		results.append(
			{
				"epa_site_id": epa["siteid"],
				"epa_name": epa["sitename"],
				"cwa_station_id": nearest_station["StationId"],
				"cwa_station_name": nearest_station["StationName"],
				"distance_km": round(nearest_distance, 2),
			}
		)

	return pd.DataFrame(
		results,
		columns=[
			"epa_site_id",
			"epa_name",
			"cwa_station_id",
			"cwa_station_name",
			"distance_km",
		],
	)


def save_station_mapping_to_db(
	mapping_df: pd.DataFrame,
	database_url: str,
	table_name: str = "station_mapping",
) -> None:
	"""Create/replace station mapping table in PostgreSQL."""
	if mapping_df.empty:
		raise ValueError("station mapping dataframe is empty; nothing to save.")

	engine = create_engine(database_url)
	try:
		mapping_df.to_sql(table_name, con=engine, if_exists="replace", index=False)
	finally:
		engine.dispose()


if __name__ == "__main__":
	load_dotenv(BASE_DIR / ".env")

	cwa_key = os.getenv("CWA_API_KEY", "")
	aqi_key = os.getenv("API_KEY")
	database_url = os.getenv("DATABASE_URL", "")

	if not cwa_key:
		raise SystemExit("Missing CWA_API_KEY environment variable.")
	if not aqi_key:
		raise SystemExit("Missing API_KEY environment variable.")

	mapping_df = build_station_weather_mapping(cwa_api_key=cwa_key, moenv_api_key=aqi_key)
	print(mapping_df.head())

	if database_url:
		save_station_mapping_to_db(mapping_df, database_url=database_url)
		print("Saved mapping table to PostgreSQL: station_mapping")
	else:
		print("DATABASE_URL not set; skipped DB write.")
