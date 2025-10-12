"""
Weather Observations Pipeline DAG.

Runs every 15 minutes to ensure fresh observation data for real-time energy forecasting.
While DWD synop reports update every 30 minutes, BrightSky polls every 10 minutes,
so 15-minute updates provide near real-time data availability.

Schedule: Every 15 minutes
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task, task_group


@dag(
    dag_id="weather_observations_pipeline",
    description="Real-time weather observations pipeline (15-min updates)",
    schedule="*/15 * * * *",  # Every 15 minutes (captures all DWD synop reports)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["weather", "observations", "real-time"],
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
)
def weather_observations_pipeline():
    """Real-time weather observations pipeline."""

    @task_group()
    def extract_and_load():
        """Extract observation data from BrightSky API and load into raw storage."""

        @task(task_id="ingest_raw_observations")
        def ingest_raw_observations():
            """
            Fetch raw weather observation data from BrightSky API.

            - Fetches today only (observations)
            - Uses 5 parallel workers for faster ingestion
            - Stores complete JSON responses in raw_api_responses table
            - Database handles duplicates with ON CONFLICT DO NOTHING

            Runs every 15 minutes to ensure near real-time data availability.
            """
            from ingest_weather_raw import BrightSkyClient, ingest_raw_weather_data
            from config import config

            client = BrightSkyClient(
                config.api.base_url,
                config.api.timeout,
                config.api.retry_attempts
            )

            # Fetch only today for observations (no need for tomorrow's observations)
            today = datetime.utcnow().strftime("%Y-%m-%d")

            count = ingest_raw_weather_data(
                client,
                date=today,
                max_workers=5,      # Parallel API requests
                batch_size=100      # Database batch size
            )

            return {
                "date": today,
                "raw_responses_stored": count,
                "timestamp": datetime.utcnow().isoformat()
            }

        @task(task_id="transform_raw_to_structured")
        def transform_raw_to_structured(extract_stats: dict):
            """
            Transform raw JSON responses to structured observation tables.

            SQL-based transformations:
            - Extract observations from JSONB → raw_weather_observations
            - Skip forecasts (handled by forecasts pipeline)
            - Skip stations (handled by stations_sync pipeline)
            - Mark processed responses
            """
            from database import transform_observations_only

            stats = transform_observations_only()

            return {
                "extract": extract_stats,
                "raw_transform": stats,
                "timestamp": datetime.utcnow().isoformat()
            }

        # Define task dependencies
        raw_data = ingest_raw_observations()
        structured_data = transform_raw_to_structured(raw_data)

        return structured_data

    @task_group()
    def transform(load_stats: dict):
        """Transform structured observation data into ML-ready format."""

        @task(task_id="aggregate_observations")
        def aggregate_observations(upstream_stats: dict):
            """
            Aggregate observations by postal code.

            - Spatial weighting by distance to stations
            - Outlier detection and quality scoring
            - Aggregation to hourly postal code level
            - Output: weather_observations_by_postal_code

            Note: Uses precomputed distances from postal_code_nearest_stations
            (refreshed by stations_sync DAG every 6 hours)
            """
            from transform_weather import WeatherTransformer

            transformer = WeatherTransformer()
            obs_count = transformer.transform_observations()

            return {
                "observations_transformed": obs_count,
                "timestamp": datetime.utcnow().isoformat()
            }

        @task(task_id="summarize_pipeline")
        def summarize_pipeline(extract_stats: dict, obs_stats: dict):
            """Summarize observations pipeline execution."""
            import logging
            logger = logging.getLogger(__name__)

            summary = {
                "extract_and_load": extract_stats,
                "observations": obs_stats,
                "pipeline_completed_at": datetime.utcnow().isoformat()
            }

            logger.info(f"✓ Observations pipeline completed successfully")
            logger.info(f"  - Raw responses: {extract_stats.get('raw_responses_stored', 0)}")
            logger.info(f"  - Observations: {obs_stats.get('observations_transformed', 0)} records")

            return summary

        # Define task dependencies
        obs_result = aggregate_observations(load_stats)
        pipeline_summary = summarize_pipeline(load_stats, obs_result)

        return pipeline_summary

    @task_group()
    def quality_checks(transform_stats: dict):
        """Run data quality checks on ML-ready observation data."""

        @task(task_id="run_quality_checks")
        def run_quality_checks(upstream_stats: dict):
            """
            Run comprehensive data quality checks on observations table.

            Checks include:
            - Data freshness (max age: 1 hour)
            - Temporal continuity (gap detection)
            - Data completeness (core fields ≥80%)
            - Quality score validation
            - Spatial coverage (≥5 postal codes)
            - Anomaly detection (IQR-based)
            """
            from data_quality_checks import DataQualityChecker

            checker = DataQualityChecker()
            results = checker.run_all_checks_observations()

            return {
                "quality_checks": results,
                "upstream": upstream_stats,
                "timestamp": datetime.utcnow().isoformat()
            }

        @task(task_id="summarize_with_checks")
        def summarize_with_checks(quality_results: dict):
            """Summarize pipeline including quality check results."""
            import logging
            logger = logging.getLogger(__name__)

            checks = quality_results["quality_checks"]
            success_rate = checks.get("success_rate", 0)

            logger.info(f"✓ Data quality checks completed")
            logger.info(f"  - Success rate: {success_rate:.0%}")
            logger.info(f"  - Checks passed: {checks.get('checks_passed', 0)}/{checks.get('total_checks', 0)}")

            if checks.get("warnings"):
                logger.warning(f"  - Warnings: {len(checks['warnings'])} found")

            if checks.get("errors"):
                logger.error(f"  - Errors: {len(checks['errors'])} found")

            return quality_results

        # Define task dependencies
        check_results = run_quality_checks(transform_stats)
        summary = summarize_with_checks(check_results)

        return summary

    # Main pipeline flow
    extract_load_output = extract_and_load()
    transform_output = transform(extract_load_output)
    quality_checks(transform_output)


# Instantiate the DAG
weather_observations_pipeline()