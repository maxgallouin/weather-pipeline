"""
Weather Forecasts Pipeline DAG.

Runs every hour to provide fresh forecast data for energy demand prediction.
While DWD NWP models update every 3 hours, hourly updates ensure timely
incorporation of the latest forecasts into ML models.

Schedule: Every hour
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task, task_group


@dag(
    dag_id="weather_forecasts_pipeline",
    description="Weather forecasts pipeline (hourly updates)",
    schedule="15 * * * *",  # Every hour at :15 past
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["weather", "forecasts", "nwp"],
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
)
def weather_forecasts_pipeline():
    """Weather forecasts pipeline for NWP data."""

    @task_group()
    def extract_and_load():
        """Extract forecast data from BrightSky API and load into raw storage."""

        @task(task_id="ingest_raw_forecasts")
        def ingest_raw_forecasts():
            """
            Fetch raw weather forecast data from BrightSky API.

            - Fetches configurable forecast horizon (default: 10 days)
            - Uses 5 parallel workers for faster ingestion
            - Stores complete JSON responses in raw_api_responses table
            - Database handles duplicates with ON CONFLICT DO NOTHING

            Runs hourly to ensure timely availability of latest forecasts.
            """
            from ingest_weather_raw import BrightSkyClient, ingest_raw_weather_data
            from config import config

            client = BrightSkyClient(
                config.api.base_url,
                config.api.timeout,
                config.api.retry_attempts
            )

            # Fetch forecast horizon (configurable via FORECAST_HORIZON_DAYS)
            dates = []
            total_count = 0

            for days_ahead in range(config.app.forecast_horizon_days):
                date = (datetime.utcnow() + timedelta(days=days_ahead)).strftime("%Y-%m-%d")
                dates.append(date)

                count = ingest_raw_weather_data(
                    client,
                    date=date,
                    max_workers=5,      # Parallel API requests
                    batch_size=100      # Database batch size
                )
                total_count += count

            return {
                "dates": dates,
                "date_range": f"{dates[0]} to {dates[-1]}",
                "raw_responses_stored": total_count,
                "timestamp": datetime.utcnow().isoformat()
            }

        @task(task_id="transform_raw_to_structured")
        def transform_raw_to_structured(extract_stats: dict):
            """
            Transform raw JSON responses to structured forecast tables.

            SQL-based transformations:
            - Extract forecasts from JSONB → raw_weather_forecasts
            - Skip observations (handled by observations pipeline)
            - Skip stations (handled by stations_sync pipeline)
            - Mark processed responses
            """
            from database import transform_forecasts_only

            stats = transform_forecasts_only()

            return {
                "extract": extract_stats,
                "raw_transform": stats,
                "timestamp": datetime.utcnow().isoformat()
            }

        # Define task dependencies
        raw_data = ingest_raw_forecasts()
        structured_data = transform_raw_to_structured(raw_data)

        return structured_data

    @task_group()
    def transform(load_stats: dict):
        """Transform structured forecast data into ML-ready format."""

        @task(task_id="aggregate_forecasts")
        def aggregate_forecasts(upstream_stats: dict):
            """
            Aggregate forecasts by postal code.

            - Processes 6-hour window of recent forecasts
            - Spatial weighting by distance to stations
            - Quality scoring and completeness checks
            - Aggregation to hourly postal code level
            - Output: weather_forecasts_by_postal_code

            Note: Uses precomputed distances from postal_code_nearest_stations
            (refreshed by stations_sync DAG every 6 hours)
            """
            from transform_weather import WeatherTransformer

            transformer = WeatherTransformer()
            fcst_count = transformer.transform_forecasts()

            return {
                "forecasts_transformed": fcst_count,
                "timestamp": datetime.utcnow().isoformat()
            }

        @task(task_id="summarize_pipeline")
        def summarize_pipeline(extract_stats: dict, fcst_stats: dict):
            """Summarize forecasts pipeline execution."""
            import logging
            logger = logging.getLogger(__name__)

            summary = {
                "extract_and_load": extract_stats,
                "forecasts": fcst_stats,
                "pipeline_completed_at": datetime.utcnow().isoformat()
            }

            logger.info(f"✓ Forecasts pipeline completed successfully")
            logger.info(f"  - Date range: {extract_stats.get('date_range', 'N/A')}")
            logger.info(f"  - Raw responses: {extract_stats.get('raw_responses_stored', 0)}")
            logger.info(f"  - Forecasts: {fcst_stats.get('forecasts_transformed', 0)} records")

            return summary

        # Define task dependencies
        fcst_result = aggregate_forecasts(load_stats)
        pipeline_summary = summarize_pipeline(load_stats, fcst_result)

        return pipeline_summary

    @task_group()
    def quality_checks(transform_stats: dict):
        """Run data quality checks on ML-ready forecast data."""

        @task(task_id="run_quality_checks")
        def run_quality_checks(upstream_stats: dict):
            """
            Run comprehensive data quality checks on forecasts table.

            Checks include:
            - Data freshness (max age: 2 hours)
            - Temporal continuity (gap detection in 48h window)
            - Data completeness (core fields ≥80%)
            - Quality score validation
            - Spatial coverage (≥5 postal codes)
            - Anomaly detection (IQR-based)
            """
            from data_quality_checks import DataQualityChecker

            checker = DataQualityChecker()
            results = checker.run_all_checks_forecasts()

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
weather_forecasts_pipeline()