"""
Weather Stations Sync DAG.

Syncs weather station metadata from BrightSky API.
Since station metadata changes infrequently (DWD updates daily, new stations added irregularly),
this DAG runs every 6 hours to keep station information current without excessive overhead.

Schedule: Every 6 hours
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator


@dag(
    dag_id="weather_stations_sync",
    description="Weather station metadata sync (6-hour updates)",
    schedule="0 */6 * * *",  # Every 6 hours at the top of the hour
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["weather", "stations", "metadata"],
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
)
def weather_stations_sync():
    """Weather station metadata sync pipeline."""

    @task(task_id="fetch_station_metadata")
    def fetch_station_metadata():
        """
        Fetch station metadata by requesting weather data.

        Since BrightSky doesn't have a dedicated stations endpoint,
        we fetch a minimal weather request to get station metadata from the response.
        The raw API response contains station information in the 'sources' field.
        """
        from ingest_weather_raw import BrightSkyClient, ingest_raw_weather_data
        from config import config

        client = BrightSkyClient(
            config.api.base_url,
            config.api.timeout,
            config.api.retry_attempts
        )

        # Fetch today's data to get station metadata
        # This will pull station info from all available stations
        today = datetime.utcnow().strftime("%Y-%m-%d")

        count = ingest_raw_weather_data(
            client,
            date=today,
            max_workers=5,
            batch_size=100
        )

        return {
            "date": today,
            "raw_responses_stored": count,
            "timestamp": datetime.utcnow().isoformat()
        }

    @task(task_id="transform_stations")
    def transform_stations(fetch_stats: dict):
        """
        Transform raw API responses to extract and upsert station metadata.

        Processes the 'sources' field from raw API responses and updates:
        - Station IDs and names
        - Geographic locations (lat/lon)
        - Observation types
        - Operational periods (first_record, last_record)
        """
        from database import transform_raw_to_stations, mark_raw_responses_processed

        stations_count = transform_raw_to_stations()
        marked_count = mark_raw_responses_processed()

        return {
            "fetch": fetch_stats,
            "stations_updated": stations_count,
            "responses_processed": marked_count,
            "timestamp": datetime.utcnow().isoformat()
        }

    # Refresh distance matrix after station updates
    refresh_distances = PostgresOperator(
        task_id="refresh_distances",
        postgres_conn_id="postgres_default",
        sql="refresh_nearest_stations.sql",
        autocommit=True,
    )

    @task(task_id="summarize_sync")
    def summarize_sync(transform_stats: dict):
        """Summarize station sync execution."""
        import logging
        logger = logging.getLogger(__name__)

        summary = {
            "stations_sync": transform_stats,
            "completed_at": datetime.utcnow().isoformat()
        }

        logger.info(f"✓ Station sync completed successfully")
        logger.info(f"  - Date: {transform_stats['fetch'].get('date', 'N/A')}")
        logger.info(f"  - Stations updated: {transform_stats.get('stations_updated', 0)}")
        logger.info(f"  - Distance matrix refreshed")

        return summary

    # Define task dependencies
    fetch_result = fetch_station_metadata()
    transform_result = transform_stations(fetch_result)

    # Refresh distances after stations are updated, then summarize
    transform_result >> refresh_distances >> summarize_sync(transform_result)


# Instantiate the DAG
weather_stations_sync()