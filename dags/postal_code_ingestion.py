"""
Airflow DAG for postal code ingestion.

Runs daily to keep postal code boundaries up to date.
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task

# Add src to Python path
sys.path.insert(0, str(Path("/opt/airflow")))

from src.ingest_postal_codes import main as ingest_postal_codes_main


@dag(
    dag_id="postal_code_ingestion",
    description="Ingest German postal code boundaries from yetzt/postleitzahlen",
    schedule="0 2 * * *",  # Daily at 2 AM
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["ingestion", "geospatial"],
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
)
def postal_code_ingestion_dag():
    """DAG for ingesting postal code boundaries."""

    @task
    def ingest_postal_codes():
        """
        Ingest German postal code boundaries from yetzt/postleitzahlen GitHub repo.

        Source: https://github.com/yetzt/postleitzahlen
        Data extracted from OpenStreetMap via Overpass.
        """
        ingest_postal_codes_main()

    ingest_postal_codes()


postal_code_ingestion_dag()