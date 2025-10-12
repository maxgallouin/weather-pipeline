"""
Weather data transformation and validation pipeline.

Transforms raw weather data into ML-ready format with:
- Data cleaning and validation
- Spatial aggregation to postal code level
- Quality scoring
- Outlier detection
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

try:
    from src.config import config
    from src.database import execute_query, fetch_all, get_db_connection
except ImportError:
    from config import config
    from database import execute_query, fetch_all, get_db_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Physical constraints for weather variables (for outlier detection)
WEATHER_CONSTRAINTS = {
    "temperature": (-50.0, 50.0),  # °C
    "precipitation": (0.0, 300.0),  # mm/h (extreme: 305mm in 1h recorded)
    "pressure_msl": (870.0, 1085.0),  # hPa (records: 870 low, 1084 high)
    "relative_humidity": (0.0, 100.0),  # %
    "wind_speed": (0.0, 408.0),  # km/h (record: 408 km/h)
    "wind_gust_speed": (0.0, 408.0),  # km/h
    "cloud_cover": (0, 100),  # %
    "visibility": (0.0, 100000.0),  # meters
    "sunshine": (0.0, 60.0),  # minutes per hour
}


class WeatherTransformer:
    """Transforms raw weather data into ML-ready format."""

    def __init__(self):
        self.stats = {"processed": 0, "failed": 0, "outliers_detected": 0}

    def validate_value(self, field: str, value: Optional[float]) -> Tuple[bool, bool]:
        """
        Validate a weather measurement against physical constraints.

        Args:
            field: Field name
            value: Measured value

        Returns:
            Tuple of (is_valid, is_outlier)
        """
        if value is None:
            return True, False  # Null is valid but not an outlier

        if field not in WEATHER_CONSTRAINTS:
            return True, False

        min_val, max_val = WEATHER_CONSTRAINTS[field]
        is_outlier = value < min_val or value > max_val

        return True, is_outlier

    def detect_outliers_in_record(self, record: Dict) -> Tuple[bool, List[str]]:
        """
        Detect outliers in a weather record.

        Args:
            record: Weather record dictionary

        Returns:
            Tuple of (has_outliers, list_of_outlier_fields)
        """
        outlier_fields = []

        for field in WEATHER_CONSTRAINTS.keys():
            if field in record:
                _, is_outlier = self.validate_value(field, record[field])
                if is_outlier:
                    outlier_fields.append(field)

        return len(outlier_fields) > 0, outlier_fields

    def calculate_data_completeness(self, record: Dict) -> float:
        """
        Calculate data completeness score (0.0 to 1.0).

        Args:
            record: Weather record

        Returns:
            Completeness score
        """
        core_fields = [
            "temperature",
            "precipitation",
            "pressure_msl",
            "relative_humidity",
            "wind_speed",
            "cloud_cover",
        ]

        non_null_count = sum(1 for field in core_fields if record.get(field) is not None)
        return non_null_count / len(core_fields)

    def calculate_quality_score(
        self, completeness: float, has_outliers: bool, num_stations: int
    ) -> float:
        """
        Calculate overall data quality score.

        Args:
            completeness: Data completeness (0-1)
            has_outliers: Whether record has outliers
            num_stations: Number of stations contributing

        Returns:
            Quality score (0.0 to 1.0)
        """
        # Start with completeness
        score = completeness

        # Penalize outliers
        if has_outliers:
            score *= 0.7

        # Boost score if multiple stations contribute
        if num_stations >= 3:
            score *= 1.1
        elif num_stations >= 2:
            score *= 1.05

        return min(score, 1.0)

    def calculate_feels_like_temperature(
        self, temp: Optional[float], humidity: Optional[float], wind_speed: Optional[float]
    ) -> Optional[float]:
        """
        Calculate "feels like" temperature using heat index and wind chill.

        Uses Steadman's heat index formula for warm/humid conditions and
        JAG/TI wind chill formula for cold/windy conditions.

        Args:
            temp: Temperature in °C
            humidity: Relative humidity in %
            wind_speed: Wind speed in km/h

        Returns:
            Feels-like temperature in °C
        """
        if temp is None:
            return None

        # Wind chill for cold temperatures (JAG/TI formula, valid for T < 10°C, wind > 4.8 km/h)
        if temp < 10 and wind_speed is not None and wind_speed > 4.8:
            wind_chill = (
                13.12
                + 0.6215 * temp
                - 11.37 * (wind_speed ** 0.16)
                + 0.3965 * temp * (wind_speed ** 0.16)
            )
            return round(wind_chill, 1)

        # Heat index for warm temperatures (Steadman formula, valid for T > 27°C)
        if temp > 27 and humidity is not None:
            # Convert to Fahrenheit for the formula
            temp_f = temp * 9/5 + 32

            # Steadman heat index formula (simplified regression)
            hi_f = (
                -42.379
                + 2.04901523 * temp_f
                + 10.14333127 * humidity
                - 0.22475541 * temp_f * humidity
                - 6.83783e-3 * temp_f * temp_f
                - 5.481717e-2 * humidity * humidity
                + 1.22874e-3 * temp_f * temp_f * humidity
                + 8.5282e-4 * temp_f * humidity * humidity
                - 1.99e-6 * temp_f * temp_f * humidity * humidity
            )

            # Convert back to Celsius
            hi_c = (hi_f - 32) * 5/9
            return round(hi_c, 1)

        # Otherwise return actual temperature
        return temp

    def classify_precipitation_intensity(self, precip: Optional[float]) -> str:
        """
        Classify precipitation intensity.

        Args:
            precip: Precipitation in mm/h

        Returns:
            Intensity classification
        """
        if precip is None or precip < 0.1:
            return "none"
        elif precip < 2.5:
            return "light"
        elif precip < 10.0:
            return "moderate"
        else:
            return "heavy"

    def transform_observations(
        self, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None
    ) -> int:
        """
        Transform raw observations into postal code aggregates.

        Args:
            start_time: Start of time window
            end_time: End of time window

        Returns:
            Number of records processed
        """
        # Default to last 24 hours if not specified
        if end_time is None:
            end_time = datetime.utcnow()
        if start_time is None:
            start_time = end_time - timedelta(hours=24)

        logger.info(f"Transforming observations from {start_time} to {end_time}")

        # Query to aggregate weather data to postal code level
        # Uses precomputed distances for inverse distance weighting
        query = """
        WITH postal_code_weather AS (
            SELECT
                pcns.postal_code,
                DATE_TRUNC('hour', rwo.timestamp) as hour,
                pcns.station_id,
                rwo.temperature,
                rwo.precipitation,
                rwo.pressure_msl,
                rwo.relative_humidity,
                rwo.wind_speed,
                rwo.wind_direction,
                rwo.wind_gust_speed,
                rwo.cloud_cover,
                rwo.visibility,
                rwo.sunshine,
                rwo.solar,
                rwo.dew_point,
                rwo.condition,
                pcns.distance_km,
                pcns.weight
            FROM postal_code_nearest_stations pcns
            JOIN raw_weather_observations rwo ON rwo.station_id = pcns.station_id
            WHERE pcns.observation_type IN ('current', 'synop')
              AND pcns.rank <= 3
              AND rwo.timestamp >= %s
              AND rwo.timestamp < %s
              AND rwo.is_validated = FALSE
        )
        SELECT
            postal_code,
            hour as timestamp,
            -- Weighted averages (normalize weights at query time to handle missing data)
            SUM(temperature * weight) / NULLIF(SUM(weight), 0) as temperature,
            SUM(precipitation * weight) / NULLIF(SUM(weight), 0) as precipitation,
            SUM(pressure_msl * weight) / NULLIF(SUM(weight), 0) as pressure_msl,
            SUM(relative_humidity * weight) / NULLIF(SUM(weight), 0) as relative_humidity,
            SUM(wind_speed * weight) / NULLIF(SUM(weight), 0) as wind_speed,
            -- Circular mean for wind direction
            DEGREES(ATAN2(
                SUM(SIN(RADIANS(wind_direction)) * weight),
                SUM(COS(RADIANS(wind_direction)) * weight)
            ))::INTEGER as wind_direction,
            SUM(wind_gust_speed * weight) / NULLIF(SUM(weight), 0) as wind_gust_speed,
            ROUND(SUM(cloud_cover * weight) / NULLIF(SUM(weight), 0))::INTEGER as cloud_cover,
            SUM(visibility * weight) / NULLIF(SUM(weight), 0) as visibility,
            SUM(sunshine * weight) / NULLIF(SUM(weight), 0) as sunshine,
            SUM(solar * weight) / NULLIF(SUM(weight), 0) as solar,
            SUM(dew_point * weight) / NULLIF(SUM(weight), 0) as dew_point,
            -- Metadata
            COUNT(DISTINCT station_id) as num_stations,
            AVG(distance_km) as avg_distance_km,
            MODE() WITHIN GROUP (ORDER BY condition) as weather_condition
        FROM postal_code_weather
        GROUP BY postal_code, hour
        HAVING COUNT(*) >= 1
        ORDER BY postal_code, hour
        """

        records = fetch_all(query, (start_time, end_time))
        logger.info(f"Got {len(records)} aggregated records to process")

        # Process and insert transformed records
        processed_count = 0
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                for record in records:
                    try:
                        # Data validation
                        has_outliers, _ = self.detect_outliers_in_record(record)
                        completeness = self.calculate_data_completeness(record)
                        quality = self.calculate_quality_score(
                            completeness, has_outliers, record.get("num_stations", 1)
                        )

                        # Derived features
                        feels_like = self.calculate_feels_like_temperature(
                            record.get("temperature"),
                            record.get("relative_humidity"),
                            record.get("wind_speed"),
                        )

                        precip_intensity = self.classify_precipitation_intensity(
                            record.get("precipitation")
                        )

                        # Insert transformed record
                        insert_query = """
                            INSERT INTO weather_observations_by_postal_code (
                                postal_code, timestamp, temperature, precipitation,
                                pressure_msl, relative_humidity, wind_speed, wind_direction,
                                wind_gust_speed, cloud_cover, visibility, sunshine, solar,
                                dew_point, temperature_feels_like, precipitation_intensity,
                                weather_condition, num_stations_used, avg_station_distance_km,
                                data_completeness, quality_score, has_imputed_values
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s
                            )
                            ON CONFLICT (postal_code, timestamp) DO UPDATE SET
                                temperature = EXCLUDED.temperature,
                                precipitation = EXCLUDED.precipitation,
                                quality_score = EXCLUDED.quality_score,
                                processed_at = CURRENT_TIMESTAMP
                        """

                        cur.execute(
                            insert_query,
                            (
                                record["postal_code"],
                                record["timestamp"],
                                record.get("temperature"),
                                record.get("precipitation"),
                                record.get("pressure_msl"),
                                record.get("relative_humidity"),
                                record.get("wind_speed"),
                                record.get("wind_direction"),
                                record.get("wind_gust_speed"),
                                record.get("cloud_cover"),
                                record.get("visibility"),
                                record.get("sunshine"),
                                record.get("solar"),
                                record.get("dew_point"),
                                feels_like,
                                precip_intensity,
                                record.get("weather_condition"),
                                record.get("num_stations"),
                                record.get("avg_distance_km"),
                                completeness,
                                quality,
                                False,  # has_imputed_values
                            ),
                        )

                        processed_count += 1

                        if has_outliers:
                            self.stats["outliers_detected"] += 1

                    except Exception as e:
                        logger.error(f"Error processing record: {e}")
                        self.stats["failed"] += 1
                        continue

        # Mark raw records as validated
        update_query = """
            UPDATE raw_weather_observations
            SET is_validated = TRUE
            WHERE timestamp >= %s AND timestamp < %s
        """
        execute_query(update_query, (start_time, end_time))

        logger.info(
            f"✓ Transformed {processed_count} observation records "
            f"({self.stats['outliers_detected']} with outliers)"
        )
        return processed_count

    def transform_forecasts(
        self, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None
    ) -> int:
        """
        Transform raw forecasts into postal code aggregates.

        Args:
            start_time: Start of time window
            end_time: End of time window

        Returns:
            Number of records processed
        """
        # Similar to observations but for forecasts
        # Get most recent forecast run (use configured horizon)
        if end_time is None:
            end_time = datetime.utcnow() + timedelta(days=config.app.forecast_horizon_days)
        if start_time is None:
            start_time = datetime.utcnow()

        logger.info(f"Transforming forecasts from {start_time} to {end_time}")

        # Get the latest forecast creation time window (last 6 hours of forecasts)
        latest_forecast_query = """
            SELECT
                MAX(forecast_created_at) as latest,
                MAX(forecast_created_at) - INTERVAL '6 hours' as earliest
            FROM raw_weather_forecasts
            WHERE is_validated = FALSE
        """
        result = fetch_all(latest_forecast_query)

        if not result or not result[0]["latest"]:
            logger.warning("No new forecasts to transform")
            return 0

        latest_forecast_time = result[0]["latest"]
        earliest_forecast_time = result[0]["earliest"]
        logger.info(f"Processing forecasts created between {earliest_forecast_time} and {latest_forecast_time}")

        # Process all forecasts using precomputed distances
        query = """
        WITH latest_forecast_per_postal AS (
            -- Find the most recent forecast_created_at for each postal code + hour combination
            SELECT
                pcns.postal_code,
                DATE_TRUNC('hour', rwf.timestamp) as hour,
                MAX(rwf.forecast_created_at) as latest_forecast_created_at
            FROM postal_code_nearest_stations pcns
            JOIN raw_weather_forecasts rwf ON rwf.station_id = pcns.station_id
            WHERE pcns.observation_type = 'forecast'
              AND pcns.rank <= 3
              AND rwf.forecast_created_at >= %s
              AND rwf.forecast_created_at <= %s
              AND rwf.timestamp >= %s
              AND rwf.timestamp < %s
              AND rwf.is_validated = FALSE
            GROUP BY pcns.postal_code, DATE_TRUNC('hour', rwf.timestamp)
        ),
        postal_code_forecast AS (
            SELECT
                pcns.postal_code,
                DATE_TRUNC('hour', rwf.timestamp) as hour,
                lfp.latest_forecast_created_at as forecast_created_at,
                pcns.station_id,
                EXTRACT(EPOCH FROM (rwf.timestamp - lfp.latest_forecast_created_at)) / 3600.0 as horizon_hours,
                rwf.temperature,
                rwf.precipitation,
                rwf.pressure_msl,
                rwf.relative_humidity,
                rwf.wind_speed,
                rwf.wind_direction,
                rwf.wind_gust_speed,
                rwf.cloud_cover,
                rwf.visibility,
                rwf.sunshine,
                rwf.solar,
                rwf.dew_point,
                rwf.precipitation_probability,
                rwf.condition,
                pcns.distance_km,
                pcns.weight
            FROM postal_code_nearest_stations pcns
            JOIN raw_weather_forecasts rwf ON rwf.station_id = pcns.station_id
            JOIN latest_forecast_per_postal lfp
                ON lfp.postal_code = pcns.postal_code
                AND lfp.hour = DATE_TRUNC('hour', rwf.timestamp)
                AND lfp.latest_forecast_created_at = rwf.forecast_created_at
            WHERE pcns.observation_type = 'forecast'
              AND pcns.rank <= 3
              AND rwf.timestamp >= %s
              AND rwf.timestamp < %s
              AND rwf.is_validated = FALSE
        )
        SELECT
            postal_code,
            hour as timestamp,
            forecast_created_at,
            ROUND(AVG(horizon_hours))::INTEGER as forecast_horizon_hours,
            -- Weighted averages (normalize weights at query time to handle missing data)
            SUM(temperature * weight) / NULLIF(SUM(weight), 0) as temperature,
            SUM(precipitation * weight) / NULLIF(SUM(weight), 0) as precipitation,
            SUM(pressure_msl * weight) / NULLIF(SUM(weight), 0) as pressure_msl,
            SUM(relative_humidity * weight) / NULLIF(SUM(weight), 0) as relative_humidity,
            SUM(wind_speed * weight) / NULLIF(SUM(weight), 0) as wind_speed,
            DEGREES(ATAN2(
                SUM(SIN(RADIANS(wind_direction)) * weight),
                SUM(COS(RADIANS(wind_direction)) * weight)
            ))::INTEGER as wind_direction,
            SUM(wind_gust_speed * weight) / NULLIF(SUM(weight), 0) as wind_gust_speed,
            ROUND(SUM(cloud_cover * weight) / NULLIF(SUM(weight), 0))::INTEGER as cloud_cover,
            SUM(visibility * weight) / NULLIF(SUM(weight), 0) as visibility,
            SUM(sunshine * weight) / NULLIF(SUM(weight), 0) as sunshine,
            SUM(solar * weight) / NULLIF(SUM(weight), 0) as solar,
            SUM(dew_point * weight) / NULLIF(SUM(weight), 0) as dew_point,
            ROUND(SUM(precipitation_probability * weight) / NULLIF(SUM(weight), 0))::INTEGER as precipitation_probability,
            COUNT(DISTINCT station_id) as num_stations,
            AVG(distance_km) as avg_distance_km,
            MODE() WITHIN GROUP (ORDER BY condition) as weather_condition
        FROM postal_code_forecast
        GROUP BY postal_code, hour, forecast_created_at
        ORDER BY postal_code, hour
        """

        records = fetch_all(query, (earliest_forecast_time, latest_forecast_time,
                                   start_time, end_time, start_time, end_time))
        logger.info(f"Got {len(records)} aggregated forecast records to process")

        # Process and insert transformed records
        processed_count = 0
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                for record in records:
                    try:
                        has_outliers, _ = self.detect_outliers_in_record(record)
                        completeness = self.calculate_data_completeness(record)
                        quality = self.calculate_quality_score(
                            completeness, has_outliers, record.get("num_stations", 1)
                        )

                        feels_like = self.calculate_feels_like_temperature(
                            record.get("temperature"),
                            record.get("relative_humidity"),
                            record.get("wind_speed"),
                        )

                        precip_intensity = self.classify_precipitation_intensity(
                            record.get("precipitation")
                        )

                        insert_query = """
                            INSERT INTO weather_forecasts_by_postal_code (
                                postal_code, timestamp, forecast_created_at, forecast_horizon_hours,
                                temperature, precipitation, pressure_msl, relative_humidity,
                                wind_speed, wind_direction, wind_gust_speed, cloud_cover,
                                visibility, sunshine, solar, dew_point, precipitation_probability,
                                temperature_feels_like, precipitation_intensity, weather_condition,
                                num_stations_used, avg_station_distance_km, data_completeness,
                                quality_score, has_imputed_values
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                            )
                            ON CONFLICT (postal_code, timestamp, forecast_created_at) DO UPDATE SET
                                temperature = EXCLUDED.temperature,
                                quality_score = EXCLUDED.quality_score,
                                processed_at = CURRENT_TIMESTAMP
                        """

                        cur.execute(
                            insert_query,
                            (
                                record["postal_code"],
                                record["timestamp"],
                                record["forecast_created_at"],
                                record.get("forecast_horizon_hours"),
                                record.get("temperature"),
                                record.get("precipitation"),
                                record.get("pressure_msl"),
                                record.get("relative_humidity"),
                                record.get("wind_speed"),
                                record.get("wind_direction"),
                                record.get("wind_gust_speed"),
                                record.get("cloud_cover"),
                                record.get("visibility"),
                                record.get("sunshine"),
                                record.get("solar"),
                                record.get("dew_point"),
                                record.get("precipitation_probability"),
                                feels_like,
                                precip_intensity,
                                record.get("weather_condition"),
                                record.get("num_stations"),
                                record.get("avg_distance_km"),
                                completeness,
                                quality,
                                False,
                            ),
                        )

                        processed_count += 1

                    except Exception as e:
                        logger.error(f"Error processing forecast record: {e}")
                        self.stats["failed"] += 1
                        continue

        # Mark as validated - all forecasts within the time window
        update_query = """
            UPDATE raw_weather_forecasts
            SET is_validated = TRUE
            WHERE forecast_created_at >= %s
              AND forecast_created_at <= %s
              AND is_validated = FALSE
        """
        execute_query(update_query, (earliest_forecast_time, latest_forecast_time))

        logger.info(f"✓ Transformed {processed_count} forecast records")
        return processed_count


def main() -> None:
    """Main transformation workflow."""
    logger.info("Starting weather data transformation")

    try:
        transformer = WeatherTransformer()

        # Transform observations (last 24 hours)
        obs_count = transformer.transform_observations()

        # Transform forecasts (next 10 days)
        fcst_count = transformer.transform_forecasts()

        total = obs_count + fcst_count
        logger.info(f"✓ Transformation completed: {total} total records")

    except Exception as e:
        logger.error(f"✗ Transformation failed: {e}")
        raise


if __name__ == "__main__":
    main()
