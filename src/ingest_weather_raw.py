"""
Simplified weather data ingestion - stores raw JSON responses only.

This module focuses solely on fetching data from BrightSky API and storing
it as-is in JSONB format. Transformation happens separately in SQL.
"""

import argparse
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from threading import Lock
from time import sleep
from typing import Any, Dict, List

import requests

try:
    # Try relative import (when run as module)
    from src.config import config
    from src.database import bulk_insert, get_postal_codes
except ImportError:
    # Fall back to direct import (when src is in sys.path)
    from config import config
    from database import bulk_insert, get_postal_codes

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BrightSkyClient:
    """Client for BrightSky API."""

    def __init__(self, base_url: str, timeout: int = 30, retry_attempts: int = 3):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self.session = requests.Session()

    def _make_request(self, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Make a request to the API with retry logic."""
        url = f"{self.base_url}/{endpoint}"

        for attempt in range(self.retry_attempts):
            try:
                response = self.session.get(url, params=params, timeout=self.timeout)
                response.raise_for_status()
                return response.json()

            except requests.RequestException as e:
                if attempt == self.retry_attempts - 1:
                    logger.error(f"API request failed after {self.retry_attempts} attempts: {e}")
                    raise
                logger.warning(f"API request failed (attempt {attempt + 1}): {e}, retrying...")
                sleep(2**attempt)  # Exponential backoff

    def get_weather(
        self, lat: float = None, lon: float = None, date: str = None,
        dwd_station_id: str = None, max_dist: int = 50000
    ) -> Dict[str, Any]:
        """
        Get weather data for a location/station and date.

        Args:
            lat: Latitude (optional if dwd_station_id provided)
            lon: Longitude (optional if dwd_station_id provided)
            date: Date in YYYY-MM-DD format
            dwd_station_id: DWD station ID (alternative to lat/lon)
            max_dist: Maximum distance to weather stations in meters

        Returns:
            Weather data with records and sources
        """
        params = {}
        if lat is not None and lon is not None:
            params["lat"] = lat
            params["lon"] = lon
            params["max_dist"] = max_dist
        if dwd_station_id:
            params["dwd_station_id"] = dwd_station_id
        if date:
            params["date"] = date

        return self._make_request("weather", params)


def fetch_postal_code_weather(
    client: BrightSkyClient, pc: Dict[str, Any], date: str = None, index: int = 0, total: int = 0
) -> tuple:
    """
    Fetch weather data for a single postal code (thread-safe).

    Args:
        client: BrightSky API client
        pc: Postal code dict with lat/lon
        date: Date in YYYY-MM-DD format (None = current data with observations + forecasts)
        index: Current index (for logging)
        total: Total count (for logging)

    Returns:
        Tuple of (postal_code, lat, lon, date, endpoint, json_string) or None on error
    """
    try:
        logger.info(f"Fetching {pc['postal_code']} ({index+1}/{total})")

        # Fetch raw response (date=None gets current observations + forecasts)
        response = client.get_weather(pc["lat"], pc["lon"], date)

        # Return data tuple (use "current" if no date specified)
        date_value = date if date else "current"
        return (
            pc["postal_code"],
            pc["lat"],
            pc["lon"],
            date_value,
            "weather",
            json.dumps(response),
        )

    except Exception as e:
        logger.error(f"Error fetching {pc['postal_code']}: {e}")
        return None


def ingest_raw_weather_data(
    client: BrightSkyClient, date: str = None, max_workers: int = 5, batch_size: int = 100
) -> int:
    """
    Fetch and store raw API responses without any transformation (parallelized).

    Args:
        client: BrightSky API client
        date: Date in YYYY-MM-DD format (None = current data with observations + forecasts)
        max_workers: Number of parallel threads (default: 5)
        batch_size: Insert to DB every N postal codes (default: 100)

    Returns:
        Number of API responses stored
    """
    postal_codes = get_postal_codes(config.app.postal_code_filter)
    date_desc = date if date else "current"
    logger.info(
        f"Fetching weather data for {len(postal_codes)} postal codes ({date_desc}) "
        f"(using {max_workers} parallel workers)"
    )

    columns = ["postal_code", "lat", "lon", "date", "endpoint", "response_json"]
    values = []
    values_lock = Lock()
    total_inserted = 0

    def insert_batch():
        """Helper to insert current batch."""
        nonlocal total_inserted
        if values:
            on_conflict = "(postal_code, date, endpoint, fetched_at) DO NOTHING"
            count = bulk_insert("raw_api_responses", columns, values, on_conflict=on_conflict)
            total_inserted += count
            logger.info(f"✓ Batch inserted {count} responses (total: {total_inserted})")
            values.clear()

    # Parallel fetch using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        futures = {
            executor.submit(
                fetch_postal_code_weather, client, pc, date, i, len(postal_codes)
            ): pc
            for i, pc in enumerate(postal_codes)
        }

        # Process completed tasks as they finish
        for future in as_completed(futures):
            result = future.result()
            if result:
                with values_lock:
                    values.append(result)

                    # Insert batch when we hit batch_size
                    if len(values) >= batch_size:
                        insert_batch()

    # Insert remaining values
    insert_batch()

    logger.info(f"✓ Stored {total_inserted} raw API responses for {date}")
    return total_inserted


def main() -> None:
    """Main ingestion workflow."""
    parser = argparse.ArgumentParser(
        description="Fetch and store raw weather data from BrightSky API"
    )
    parser.add_argument(
        "--date",
        default=datetime.utcnow().strftime("%Y-%m-%d"),
        help="Date to fetch (YYYY-MM-DD). Defaults to today.",
    )
    parser.add_argument(
        "--days", type=int, default=1, help="Number of days to fetch (going backwards)"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=5,
        help="Number of parallel workers (default: 5, max recommended: 10)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Database insert batch size (default: 100)",
    )

    args = parser.parse_args()

    logger.info(
        f"Starting raw weather data ingestion: date={args.date}, days={args.days}, "
        f"workers={args.workers}, batch_size={args.batch_size}"
    )

    try:
        client = BrightSkyClient(
            config.api.base_url, config.api.timeout, config.api.retry_attempts
        )

        total_responses = 0

        # Fetch data for specified number of days
        for day_offset in range(args.days):
            date = (datetime.strptime(args.date, "%Y-%m-%d") - timedelta(days=day_offset)).strftime(
                "%Y-%m-%d"
            )
            count = ingest_raw_weather_data(
                client, date, max_workers=args.workers, batch_size=args.batch_size
            )
            total_responses += count

        logger.info(f"✓ Raw ingestion completed: {total_responses} API responses stored")

    except Exception as e:
        logger.error(f"✗ Raw ingestion failed: {e}")
        raise


if __name__ == "__main__":
    main()
