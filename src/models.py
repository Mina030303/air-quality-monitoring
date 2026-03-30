"""
Pydantic v2 data validation models for Taiwan Air Quality data.
Handles AQI record validation, API response parsing, and coordinate validation.
"""

from datetime import datetime
from typing import Optional, Any, Dict

from pydantic import BaseModel, Field, field_validator, model_validator


class AQIRecord(BaseModel):
    """
    Pydantic model for Air Quality Index (AQI) records.
    
    Validates all fields according to Taiwan-specific constraints and
    MOENV API data formats.
    """
    
    site_name: str = Field(
        ...,
        min_length=1,
        description="Monitoring station name (must be non-empty)"
    )
    
    county: str = Field(
        default="",
        description="County or administrative region"
    )
    
    aqi: Optional[int] = Field(
        default=None,
        ge=0,
        le=500,
        description="Air Quality Index (0-500, or None if invalid)"
    )
    
    status: str = Field(
        default="",
        description="Air quality status (e.g., '良好', '普通', '不健康')"
    )
    
    publish_time: datetime = Field(
        ...,
        description="Record publication timestamp"
    )
    
    longitude: float = Field(
        ...,
        ge=118.0,
        le=122.0,
        description="Geographic longitude (Taiwan range: 118-122)"
    )
    
    latitude: float = Field(
        ...,
        ge=21.0,
        le=26.0,
        description="Geographic latitude (Taiwan range: 21-26)"
    )
    
    class Config:
        """Pydantic model configuration."""
        str_strip_whitespace = True
        validate_assignment = True
        json_schema_extra = {
            "example": {
                "site_name": "台北監測站",
                "county": "台北市",
                "aqi": 45,
                "status": "良好",
                "publish_time": "2026-03-30T13:00:00",
                "longitude": 121.5,
                "latitude": 25.0,
            }
        }
    
    @field_validator("site_name")
    @classmethod
    def validate_site_name(cls, v: str) -> str:
        """
        Validate site_name is not empty after stripping whitespace.
        
        Args:
            v: Site name string
            
        Returns:
            Stripped site name
            
        Raises:
            ValueError: If site_name is empty
        """
        if not v or not v.strip():
            raise ValueError("site_name cannot be empty")
        return v.strip()
    
    @field_validator("aqi", mode="before")
    @classmethod
    def validate_aqi(cls, v: Any) -> Optional[int]:
        """
        Validate AQI is within valid range (0-500).
        Sets to None if invalid or out of range.
        
        Args:
            v: AQI value (can be string, int, or None)
            
        Returns:
            Integer AQI value (0-500) or None if invalid
        """
        if v is None or v == "":
            return None
        
        try:
            aqi_int = int(v)
            if 0 <= aqi_int <= 500:
                return aqi_int
            else:
                # Out of range: set to None
                return None
        except (ValueError, TypeError):
            # Cannot convert to int: set to None
            return None
    
    @field_validator("publish_time", mode="before")
    @classmethod
    def validate_publish_time(cls, v: Any) -> datetime:
        """
        Parse publish_time from various formats.
        Handles MOENV API strings like '2026/03/30 13:00' or ISO format.
        
        Args:
            v: Datetime value (string or datetime object)
            
        Returns:
            Parsed datetime object
            
        Raises:
            ValueError: If string cannot be parsed as datetime
        """
        if isinstance(v, datetime):
            return v
        
        if isinstance(v, str):
            # Try multiple datetime formats in order
            formats = [
                "%Y/%m/%d %H:%M:%S",  # MOENV format: 2026/03/30 13:00:00
                "%Y/%m/%d %H:%M",     # MOENV format: 2026/03/30 13:00
                "%Y-%m-%d %H:%M:%S",  # ISO format: 2026-03-30 13:00:00
                "%Y-%m-%d %H:%M",     # ISO format: 2026-03-30 13:00
                "%Y-%m-%dT%H:%M:%S",  # ISO 8601: 2026-03-30T13:00:00
                "%Y-%m-%dT%H:%M:%SZ", # UTC: 2026-03-30T13:00:00Z
                "%Y-%m-%d",           # Date only: 2026-03-30
            ]
            
            for fmt in formats:
                try:
                    return datetime.strptime(v.strip(), fmt)
                except ValueError:
                    continue
            
            raise ValueError(
                f"publish_time '{v}' does not match any expected format. "
                f"Expected formats: {', '.join(formats)}"
            )
        
        raise ValueError(f"publish_time must be datetime or string, got {type(v)}")
    
    @field_validator("longitude", "latitude", mode="before")
    @classmethod
    def validate_coordinates(cls, v: Any) -> float:
        """
        Convert and validate geographic coordinates.
        
        Args:
            v: Coordinate value (string or float)
            
        Returns:
            Float coordinate value
            
        Raises:
            ValueError: If cannot convert to float
        """
        try:
            return float(v)
        except (ValueError, TypeError):
            raise ValueError(f"Coordinate must be numeric, got {v}")
    
    @model_validator(mode="after")
    def validate_taiwan_bounds(self) -> "AQIRecord":
        """
        Validate that latitude and longitude are within Taiwan's geographic bounds.
        
        Returns:
            Self if validation passes
            
        Raises:
            ValueError: If coordinates are outside Taiwan bounds
        """
        if not (118.0 <= self.longitude <= 122.0):
            raise ValueError(
                f"longitude {self.longitude} outside Taiwan range (118-122)"
            )
        
        if not (21.0 <= self.latitude <= 26.0):
            raise ValueError(
                f"latitude {self.latitude} outside Taiwan range (21-26)"
            )
        
        return self
    
    @classmethod
    def from_api_json(cls, json_data: Dict[str, Any]) -> "AQIRecord":
        """
        Create AQIRecord from MOENV API JSON response.
        
        Handles mapping between API field names (often Chinese or specific keys)
        and Pydantic model fields.
        
        Supports various API response formats:
        - Direct field names: site_name, county, aqi, status, publish_time, etc.
        - Chinese field names: 測站名稱, 縣市, 空氣品質指標, 狀態, 發布時間, etc.
        - MOENV API format: sitename, publishtime, longitude, latitude, etc.
        - Nested structures with coordinate as separate fields or in dict
        
        Args:
            json_data: Dictionary from API response
            
        Returns:
            AQIRecord instance
            
        Example:
            >>> data = {
            ...     "sitename": "台北監測站",
            ...     "county": "台北市",
            ...     "aqi": 45,
            ...     "status": "良好",
            ...     "publishtime": "2026-03-30 13:00",
            ...     "longitude": 121.5,
            ...     "latitude": 25.0,
            ... }
            >>> record = AQIRecord.from_api_json(data)
        """
        # Field name mappings for common API formats
        mapping = {
            "site_name": [
                "site_name", "SiteName", "siteName",
                "sitename", "測站名稱", "station_name", "StationName"
            ],
            "county": [
                "county", "County", "縣市", "County_Name"
            ],
            "aqi": [
                "aqi", "AQI", "空氣品質指標", "AirQualityIndex"
            ],
            "status": [
                "status", "Status", "狀態", "AirQualityStatus"
            ],
            "publish_time": [
                "publish_time", "PublishTime", "publishTime",
                "publishtime", "發布時間", "PublishingDate", "RecordTime"
            ],
            "longitude": [
                "longitude", "Longitude", "經度", "lon", "Lon"
            ],
            "latitude": [
                "latitude", "Latitude", "緯度", "lat", "Lat"
            ],
        }
        
        # Extract values from API response
        extracted = {}
        
        for model_field, api_field_variants in mapping.items():
            for api_field in api_field_variants:
                if api_field in json_data:
                    extracted[model_field] = json_data[api_field]
                    break
        
        # Create and return AQIRecord instance
        return cls(**extracted)
    
    def to_db_tuple(self) -> tuple:
        """
        Convert AQIRecord to tuple format for database insertion.
        
        Returns:
            Tuple: (site_name, county, aqi, status, publish_time, longitude, latitude)
        """
        return (
            self.site_name,
            self.county,
            self.aqi,
            self.status,
            self.publish_time,
            self.longitude,
            self.latitude,
        )


class AQIRecordList(BaseModel):
    """
    Container for multiple AQI records with batch validation.
    """
    
    records: list[AQIRecord] = Field(
        default_factory=list,
        description="List of AQI records"
    )
    
    def to_db_tuples(self) -> list[tuple]:
        """
        Convert all records to database insertion format.
        
        Returns:
            List of tuples ready for database batch insert
        """
        return [record.to_db_tuple() for record in self.records]


class DailyAQIRecord(BaseModel):
    """
    Pydantic model for daily Air Quality Index (AQI) records.
    
    Validates daily aggregated AQI data from MOENV API.
    Note: Daily API endpoint (aqx_p_434) does not provide longitude/latitude,
    so these are optional fields.
    """
    
    site_name: str = Field(
        ...,
        min_length=1,
        description="Monitoring station name (must be non-empty)"
    )
    
    county: Optional[str] = Field(
        default=None,
        description="County or administrative region (optional for daily data)"
    )
    
    aqi: Optional[int] = Field(
        default=None,
        ge=0,
        le=500,
        description="Daily Air Quality Index (0-500, or None if invalid)"
    )
    
    status: Optional[str] = Field(
        default=None,
        description="Air quality status (e.g., '良好', '普通', '不健康')"
    )
    
    monitor_date: datetime = Field(
        ...,
        description="Date of monitoring (e.g., 2026-03-30)"
    )
    
    # Make coordinates optional since daily API doesn't provide them
    longitude: Optional[float] = Field(
        default=None,
        ge=118.0,
        le=122.0,
        description="Geographic longitude (Taiwan range: 118-122, optional)"
    )
    
    latitude: Optional[float] = Field(
        default=None,
        ge=21.0,
        le=26.0,
        description="Geographic latitude (Taiwan range: 21-26, optional)"
    )
    
    # Optional daily pollutant data (from daily API with "subindex" suffix)
    so2: Optional[float] = Field(
        default=None,
        description="SO2 sub-index"
    )
    
    co: Optional[float] = Field(
        default=None,
        description="CO sub-index"
    )
    
    o3: Optional[float] = Field(
        default=None,
        description="O3 sub-index"
    )
    
    pm10: Optional[float] = Field(
        default=None,
        description="PM10 sub-index"
    )
    
    pm2_5: Optional[float] = Field(
        default=None,
        alias="pm2.5",
        description="PM2.5 sub-index"
    )
    
    no2: Optional[float] = Field(
        default=None,
        description="NO2 sub-index"
    )
    
    class Config:
        """Pydantic model configuration."""
        str_strip_whitespace = True
        validate_assignment = True
        populate_by_name = True  # Allow both 'pm2_5' and 'pm2.5'
        json_schema_extra = {
            "example": {
                "site_name": "台北監測站",
                "county": "台北市",
                "aqi": 45,
                "status": "良好",
                "monitor_date": "2026-03-30",
                "longitude": None,  # Daily API doesn't provide these
                "latitude": None,
                "so2": None,  # Actual fields come from "subindex" names in API
                "pm2.5": 82,
                "pm10": 65,
                "co": 5,
                "so2": 12,
                "no2": 51,
            }
        }
    
    @field_validator("site_name")
    @classmethod
    def validate_site_name(cls, v: str) -> str:
        """Validate site_name is not empty after stripping whitespace."""
        if not v or not v.strip():
            raise ValueError("site_name cannot be empty")
        return v.strip()
    
    @field_validator("aqi", mode="before")
    @classmethod
    def validate_aqi(cls, v: Any) -> Optional[int]:
        """Validate AQI is within valid range (0-500)."""
        if v is None or v == "":
            return None
        
        try:
            aqi_int = int(v)
            if 0 <= aqi_int <= 500:
                return aqi_int
            else:
                return None
        except (ValueError, TypeError):
            return None
    
    @field_validator("monitor_date", mode="before")
    @classmethod
    def validate_monitor_date(cls, v: Any) -> datetime:
        """
        Parse monitor_date from various formats.
        Handles MOENV daily API strings like '2026-03-30' or '2026/03/30'.
        
        Args:
            v: Date value (string or datetime object)
            
        Returns:
            Parsed datetime object
            
        Raises:
            ValueError: If string cannot be parsed as date
        """
        if isinstance(v, datetime):
            return v
        
        if isinstance(v, str):
            # Try multiple date formats
            formats = [
                "%Y-%m-%d",      # ISO: 2026-03-30
                "%Y/%m/%d",      # Slash: 2026/03/30
                "%Y-%m-%d %H:%M:%S",  # With time
                "%Y/%m/%d %H:%M:%S",  # With time (slash)
            ]
            
            for fmt in formats:
                try:
                    return datetime.strptime(v.strip(), fmt)
                except ValueError:
                    continue
            
            raise ValueError(
                f"monitor_date '{v}' does not match any expected format. "
                f"Expected formats: {', '.join(formats)}"
            )
        
        raise ValueError(f"monitor_date must be datetime or string, got {type(v)}")
    
    @field_validator("longitude", "latitude", "so2", "co", "o3", "pm10", "pm2_5", "no2", mode="before")
    @classmethod
    def convert_numeric_fields(cls, v: Any) -> Optional[float]:
        """Convert numeric fields from string to float, handling empty strings."""
        if v is None or v == "":
            return None
        
        try:
            return float(v)
        except (ValueError, TypeError):
            return None
    
    @classmethod
    def from_api_json(cls, json_data: Dict[str, Any]) -> "DailyAQIRecord":
        """
        Create DailyAQIRecord from MOENV daily API JSON response.
        
        Handles the daily API format which includes:
        - siteid, sitename (site identification)
        - monitordate (YYYY-MM-DD format)  
        - aqi (daily AQI score)
        - Pollutant sub-indices with "subindex" suffix:
          o38subindex, o3subindex, pm25subindex, pm10subindex,
          cosubindex, so2subindex, no2subindex
        
        Note: Daily API does NOT provide longitude, latitude, or county.
        
        Args:
            json_data: Dictionary from MOENV daily API response
            
        Returns:
            DailyAQIRecord instance
            
        Example API response:
            {
                'siteid': '140',
                'sitename': '豐原',
                'monitordate': '2026-03-29',
                'aqi': '82',
                'o38subindex': '',
                'o3subindex': '',
                'pm25subindex': '82',
                'pm10subindex': '65',
                'cosubindex': '5',
                'so2subindex': '12',
                'no2subindex': '51'
            }
        """
        # Map daily API field names to model field names
        extracted = {}
        
        # Site name (daily API uses lowercase 'sitename')
        if "sitename" in json_data:
            extracted["site_name"] = json_data["sitename"]
        elif "site_name" in json_data:
            extracted["site_name"] = json_data["site_name"]
        
        # Monitor date
        if "monitordate" in json_data:
            extracted["monitor_date"] = json_data["monitordate"]
        elif "monitor_date" in json_data:
            extracted["monitor_date"] = json_data["monitor_date"]
        
        # AQI score
        if "aqi" in json_data:
            extracted["aqi"] = json_data["aqi"]
        
        # Status (if provided)
        if "status" in json_data:
            extracted["status"] = json_data["status"]
        
        # County (if provided - usually not in daily API)
        if "county" in json_data:
            extracted["county"] = json_data["county"]
        
        # Pollutant sub-indices (daily API uses "subindex" suffix)
        # o3subindex or o38subindex
        if "o3subindex" in json_data:
            extracted["o3"] = json_data["o3subindex"]
        elif "o38subindex" in json_data:
            extracted["o3"] = json_data["o38subindex"]
        
        # pm2.5 (daily API uses pm25subindex)
        if "pm25subindex" in json_data:
            extracted["pm2_5"] = json_data["pm25subindex"]
        
        # pm10
        if "pm10subindex" in json_data:
            extracted["pm10"] = json_data["pm10subindex"]
        
        # CO
        if "cosubindex" in json_data:
            extracted["co"] = json_data["cosubindex"]
        
        # SO2
        if "so2subindex" in json_data:
            extracted["so2"] = json_data["so2subindex"]
        
        # NO2
        if "no2subindex" in json_data:
            extracted["no2"] = json_data["no2subindex"]
        
        # Create and return DailyAQIRecord instance
        return cls(**extracted)
    
    def to_db_tuple(self) -> tuple:
        """
        Convert DailyAQIRecord to tuple format for database insertion.
        
        Returns:
            Tuple: (site_name, county, aqi, status, monitor_date, longitude, latitude, so2, co, o3, pm10, pm2_5, no2)
        """
        return (
            self.site_name,
            self.county,
            self.aqi,
            self.status,
            self.monitor_date,
            self.longitude,
            self.latitude,
            self.so2,
            self.co,
            self.o3,
            self.pm10,
            self.pm2_5,
            self.no2,
        )


class DailyAQIRecordList(BaseModel):
    """
    Container for multiple daily AQI records with batch validation.
    """
    
    records: list[DailyAQIRecord] = Field(
        default_factory=list,
        description="List of daily AQI records"
    )
    
    def to_db_tuples(self) -> list[tuple]:
        """
        Convert all records to database insertion format.
        
        Returns:
            List of tuples ready for database batch insert
        """
        return [record.to_db_tuple() for record in self.records]



if __name__ == "__main__":
    from pydantic import ValidationError
    
    print("=" * 60)
    print("Testing AQIRecord Pydantic Model")
    print("=" * 60)
    
    # Test 1: Valid record
    print("\n[Test 1] Valid record from dict:")
    try:
        record = AQIRecord(
            site_name="台北監測站",
            county="台北市",
            aqi=45,
            status="良好",
            publish_time="2026-03-30 13:00",
            longitude=121.5,
            latitude=25.0,
        )
        print(f"✓ Created: {record.site_name} - AQI: {record.aqi}")
    except ValidationError as e:
        print(f"✗ Error: {e}")
    
    # Test 2: AQI out of range
    print("\n[Test 2] AQI out of range (sets to None):")
    try:
        record = AQIRecord(
            site_name="高雄監測站",
            county="高雄市",
            aqi=999,
            status="危害",
            publish_time="2026-03-30 14:00",
            longitude=120.3,
            latitude=22.6,
        )
        print(f"✓ Created with AQI={record.aqi} (out-of-range set to None)")
    except ValidationError as e:
        print(f"✗ Error: {e}")
    
    # Test 3: Empty site_name
    print("\n[Test 3] Empty site_name (validation error):")
    try:
        record = AQIRecord(
            site_name="",
            county="台中市",
            aqi=60,
            status="普通",
            publish_time="2026-03-30 15:00",
            longitude=120.8,
            latitude=24.1,
        )
        print(f"✓ Created: {record}")
    except ValidationError as e:
        print(f"✗ Validation failed (expected): {e.error_count()} error(s)")
    
    # Test 4: Coordinates outside Taiwan range
    print("\n[Test 4] Coordinates outside Taiwan bounds:")
    try:
        record = AQIRecord(
            site_name="新竹監測站",
            county="新竹市",
            aqi=50,
            status="普通",
            publish_time="2026-03-30 16:00",
            longitude=150.0,  # Outside range
            latitude=25.0,
        )
        print(f"✓ Created: {record}")
    except ValidationError as e:
        print(f"✗ Validation failed (expected): coordinates out of bounds")
    
    # Test 5: from_api_json with Chinese field names
    print("\n[Test 5] API JSON with Chinese field names:")
    try:
        api_data = {
            "測站名稱": "台北監測站",
            "縣市": "台北市",
            "AQI": 45,
            "狀態": "良好",
            "發布時間": "2026-03-30 13:00",
            "經度": 121.5,
            "緯度": 25.0,
        }
        record = AQIRecord.from_api_json(api_data)
        print(f"✓ Parsed from API: {record.site_name} (AQI: {record.aqi})")
    except ValidationError as e:
        print(f"✗ Error: {e}")
    
    # Test 6: from_api_json with English field names
    print("\n[Test 6] API JSON with English field names:")
    try:
        api_data = {
            "SiteName": "高雄監測站",
            "County": "高雄市",
            "AQI": 78,
            "Status": "普通",
            "PublishTime": "2026-03-30 14:00",
            "Longitude": 120.3,
            "Latitude": 22.6,
        }
        record = AQIRecord.from_api_json(api_data)
        print(f"✓ Parsed from API: {record.site_name} (AQI: {record.aqi})")
    except ValidationError as e:
        print(f"✗ Error: {e}")
    
    # Test 7: DateTime parsing with various formats
    print("\n[Test 7] DateTime parsing (multiple formats):")
    formats = [
        "2026-03-30 13:00",
        "2026-03-30 13:00:30",
        "2026-03-30T13:00:00",
        "2026-03-30T13:00:00Z",
    ]
    for fmt_str in formats:
        try:
            record = AQIRecord(
                site_name="測試站",
                county="測試區",
                aqi=50,
                status="test",
                publish_time=fmt_str,
                longitude=121.0,
                latitude=25.0,
            )
            print(f"✓ Parsed '{fmt_str}' → {record.publish_time}")
        except ValidationError as e:
            print(f"✗ Failed to parse '{fmt_str}'")
    
    # Test 8: Batch processing with AQIRecordList
    print("\n[Test 8] Batch processing with AQIRecordList:")
    try:
        records_data = [
            {
                "site_name": "台北監測站",
                "county": "台北市",
                "aqi": 45,
                "status": "良好",
                "publish_time": "2026-03-30 13:00",
                "longitude": 121.5,
                "latitude": 25.0,
            },
            {
                "site_name": "高雄監測站",
                "county": "高雄市",
                "aqi": 78,
                "status": "普通",
                "publish_time": "2026-03-30 13:00",
                "longitude": 120.3,
                "latitude": 22.6,
            },
        ]
        batch = AQIRecordList(records=records_data)
        tuples = batch.to_db_tuples()
        print(f"✓ Processed {len(batch.records)} records")
        print(f"✓ Generated {len(tuples)} database tuples")
    except ValidationError as e:
        print(f"✗ Error: {e}")
    
    print("\n" + "=" * 60)
    print("All tests completed")
    print("=" * 60)
