"""Database connection and utility functions."""

import logging
from contextlib import contextmanager
from typing import Any, Dict, Generator, List, Optional

import psycopg2
import psycopg2.extras
from psycopg2 import pool
from psycopg2.extensions import connection as Connection

try:
    from src.config import config
except ImportError:
    from config import config

logger = logging.getLogger(__name__)

# Global connection pool
_connection_pool: Optional[pool.ThreadedConnectionPool] = None


def get_connection_pool() -> pool.ThreadedConnectionPool:
    """
    Get or create the connection pool singleton.

    Returns:
        ThreadedConnectionPool instance
    """
    global _connection_pool
    if _connection_pool is None:
        logger.info("Creating database connection pool")
        _connection_pool = pool.ThreadedConnectionPool(
            minconn=2,
            maxconn=10,
            host=config.database.host,
            port=config.database.port,
            database=config.database.database,
            user=config.database.user,
            password=config.database.password,
        )
    return _connection_pool


@contextmanager
def get_db_connection() -> Generator[Connection, None, None]:
    """
    Get a database connection from the pool.

    Yields:
        Database connection

    Raises:
        psycopg2.Error: If connection fails
    """
    pool_instance = get_connection_pool()
    conn = pool_instance.getconn()
    try:
        yield conn
        conn.commit()
    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        logger.error(f"Database error: {e}")
        raise
    finally:
        if conn:
            pool_instance.putconn(conn)


def execute_query(query: str, params: Optional[tuple] = None) -> None:
    """
    Execute a SQL query without returning results.

    Args:
        query: SQL query to execute
        params: Query parameters
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            logger.debug(f"Executed query: {query[:100]}...")


def fetch_one(query: str, params: Optional[tuple] = None) -> Optional[Dict[str, Any]]:
    """
    Fetch a single row as a dictionary.

    Args:
        query: SQL query
        params: Query parameters

    Returns:
        Dictionary with column names as keys, or None if no results
    """
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            result = cur.fetchone()
            return dict(result) if result else None


def fetch_all(query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
    """
    Fetch all rows as a list of dictionaries.

    Args:
        query: SQL query
        params: Query parameters

    Returns:
        List of dictionaries with column names as keys
    """
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            results = cur.fetchall()
            return [dict(row) for row in results]


def bulk_insert(
    table: str, columns: List[str], values: List[tuple], on_conflict: Optional[str] = None
) -> int:
    """
    Perform a bulk insert operation.

    Args:
        table: Table name
        columns: List of column names
        values: List of value tuples
        on_conflict: ON CONFLICT clause (e.g., "DO NOTHING" or "DO UPDATE SET ...")

    Returns:
        Number of rows inserted/updated
    """
    if not values:
        return 0

    columns_str = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"

    if on_conflict:
        query += f" ON CONFLICT {on_conflict}"

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, query, values, page_size=1000)
            row_count = cur.rowcount
            logger.info(f"Bulk inserted {row_count} rows into {table}")
            return row_count


def upsert_station(station_data: Dict[str, Any]) -> int:
    """
    Insert or update a weather station.

    Args:
        station_data: Station metadata from BrightSky API

    Returns:
        Station ID
    """
    query = """
        INSERT INTO weather_stations (
            id, dwd_station_id, station_name, observation_type,
            location, height, wmo_station_id, first_record, last_record
        ) VALUES (
            %(id)s, %(dwd_station_id)s, %(station_name)s, %(observation_type)s,
            ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326),
            %(height)s, %(wmo_station_id)s, %(first_record)s, %(last_record)s
        )
        ON CONFLICT (id) DO UPDATE SET
            dwd_station_id = EXCLUDED.dwd_station_id,
            station_name = EXCLUDED.station_name,
            observation_type = EXCLUDED.observation_type,
            location = EXCLUDED.location,
            height = EXCLUDED.height,
            last_record = EXCLUDED.last_record,
            updated_at = CURRENT_TIMESTAMP
        RETURNING id
    """

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, station_data)
            station_id = cur.fetchone()[0]
            return station_id


def batch_upsert_stations(stations_data: List[Dict[str, Any]]) -> Dict[int, int]:
    """
    Batch insert or update weather stations.

    Args:
        stations_data: List of station metadata from BrightSky API

    Returns:
        Dictionary mapping API station IDs to database station IDs
    """
    if not stations_data:
        return {}

    query = """
        INSERT INTO weather_stations (
            id, dwd_station_id, station_name, observation_type,
            location, height, wmo_station_id, first_record, last_record
        ) VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            dwd_station_id = EXCLUDED.dwd_station_id,
            station_name = EXCLUDED.station_name,
            observation_type = EXCLUDED.observation_type,
            location = EXCLUDED.location,
            height = EXCLUDED.height,
            last_record = EXCLUDED.last_record,
            updated_at = CURRENT_TIMESTAMP
        RETURNING id, dwd_station_id
    """

    # Prepare values
    values = [
        (
            s["id"],
            s["dwd_station_id"],
            s.get("station_name"),
            s.get("observation_type"),
            f"SRID=4326;POINT({s['lon']} {s['lat']})",
            s.get("height"),
            s.get("wmo_station_id"),
            s.get("first_record"),
            s.get("last_record"),
        )
        for s in stations_data
    ]

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Use execute_values for batch insert
            result = psycopg2.extras.execute_values(
                cur,
                query,
                values,
                template="(%s, %s, %s, %s, ST_GeomFromEWKT(%s), %s, %s, %s, %s)",
                fetch=True,
            )

            # Build mapping: API ID -> DB ID
            # Result contains (db_id, dwd_station_id) tuples
            # We need to map back to API IDs
            id_map = {}
            for row in result:
                db_id, dwd_id = row
                # Find the original API ID for this dwd_station_id
                for station in stations_data:
                    if station["dwd_station_id"] == dwd_id:
                        id_map[station["id"]] = db_id
                        break

            logger.info(f"Batch upserted {len(id_map)} stations")
            return id_map


def get_postal_codes(prefix_filter: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Get postal codes from database.

    Args:
        prefix_filter: Optional postal code prefix to filter by

    Returns:
        List of postal codes with their geometries
    """
    query = """
        SELECT
            postal_code,
            ST_AsText(centroid) as centroid_wkt,
            ST_X(centroid) as lon,
            ST_Y(centroid) as lat,
            area_km2
        FROM postal_codes
    """

    if prefix_filter:
        query += " WHERE postal_code LIKE %s"
        params = (f"{prefix_filter}%",)
    else:
        params = None

    return fetch_all(query, params)


def test_connection() -> bool:
    """
    Test database connection.

    Returns:
        True if connection successful, False otherwise
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                result = cur.fetchone()
                return result[0] == 1
    except Exception as e:
        logger.error(f"Connection test failed: {e}")
        return False


def transform_raw_to_stations() -> int:
    """
    Transform raw API responses to weather_stations table.

    Returns:
        Number of stations upserted
    """
    import os

    # Get absolute path to SQL file (works from any directory)
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sql_file = os.path.join(base_dir, "sql", "transform_stations.sql")

    try:
        with open(sql_file, "r") as f:
            query = f.read()

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                count = cur.rowcount
                logger.info(f"Transformed {count} stations from raw responses")
                return count
    except Exception as e:
        logger.error(f"Station transformation failed: {e}")
        raise


def transform_raw_to_observations() -> int:
    """
    Transform raw API responses to raw_weather_observations table.

    Returns:
        Number of observation records inserted
    """
    import os

    # Get absolute path to SQL file (works from any directory)
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sql_file = os.path.join(base_dir, "sql", "transform_observations.sql")

    try:
        with open(sql_file, "r") as f:
            query = f.read()

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                count = cur.rowcount
                logger.info(f"Transformed {count} observation records from raw responses")
                return count
    except Exception as e:
        logger.error(f"Observation transformation failed: {e}")
        raise


def transform_raw_to_forecasts() -> int:
    """
    Transform raw API responses to raw_weather_forecasts table.

    Returns:
        Number of forecast records inserted
    """
    import os

    # Get absolute path to SQL file (works from any directory)
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sql_file = os.path.join(base_dir, "sql", "transform_forecasts.sql")

    try:
        with open(sql_file, "r") as f:
            query = f.read()

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                count = cur.rowcount
                logger.info(f"Transformed {count} forecast records from raw responses")
                return count
    except Exception as e:
        logger.error(f"Forecast transformation failed: {e}")
        raise


def mark_raw_responses_processed() -> int:
    """
    Mark all unprocessed raw API responses as processed.

    Returns:
        Number of responses marked as processed
    """
    query = "UPDATE raw_api_responses SET processed = TRUE WHERE NOT processed"
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                count = cur.rowcount
                logger.info(f"Marked {count} raw responses as processed")
                return count
    except Exception as e:
        logger.error(f"Failed to mark responses as processed: {e}")
        raise


def transform_all_raw_data() -> Dict[str, int]:
    """
    Run all transformation steps in sequence.

    Returns:
        Dictionary with counts for each transformation step
    """
    logger.info("Starting transformation of raw API responses")

    stats = {
        "stations": transform_raw_to_stations(),
        "observations": transform_raw_to_observations(),
        "forecasts": transform_raw_to_forecasts(),
        "marked_processed": mark_raw_responses_processed(),
    }

    logger.info(f"✓ Transformation complete: {stats}")
    return stats


def transform_observations_only() -> Dict[str, int]:
    """
    Run transformation steps for observations only (skip forecasts and stations).

    Used by the observations pipeline which runs every 30 minutes.
    Station metadata is handled by the dedicated stations_sync pipeline.

    Returns:
        Dictionary with counts for each transformation step
    """
    logger.info("Starting transformation of raw API responses (observations only)")

    stats = {
        "stations": 0,  # Skip station transformation (handled by stations_sync DAG)
        "observations": transform_raw_to_observations(),
        "forecasts": 0,  # Skip forecast transformation
        "marked_processed": mark_raw_responses_processed(),
    }

    logger.info(f"✓ Observations transformation complete: {stats}")
    return stats


def transform_forecasts_only() -> Dict[str, int]:
    """
    Run transformation steps for forecasts only (skip observations and stations).

    Used by the forecasts pipeline which runs every 3 hours.
    Station metadata is handled by the dedicated stations_sync pipeline.

    Returns:
        Dictionary with counts for each transformation step
    """
    logger.info("Starting transformation of raw API responses (forecasts only)")

    stats = {
        "stations": 0,  # Skip station transformation (handled by stations_sync DAG)
        "observations": 0,  # Skip observation transformation
        "forecasts": transform_raw_to_forecasts(),
        "marked_processed": mark_raw_responses_processed(),
    }

    logger.info(f"✓ Forecasts transformation complete: {stats}")
    return stats
