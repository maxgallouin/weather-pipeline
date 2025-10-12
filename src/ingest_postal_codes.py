"""
Postal code shapes ingestion script.

Downloads German postal code geometries from the yetzt/postleitzahlen GitHub repository
(as specified in the assignment) and imports them into PostGIS.

Source: https://github.com/yetzt/postleitzahlen
Data extracted from OpenStreetMap via Overpass.
"""

import brotli
import json
import logging
from typing import Any, Dict, List
from urllib.request import urlopen

try:
    from src.config import config
    from src.database import bulk_insert, execute_query, get_db_connection
except ImportError:
    from config import config
    from database import bulk_insert, execute_query, get_db_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Official source from assignment: https://github.com/yetzt/postleitzahlen
# Contains German postal code areas extracted from OpenStreetMap
# Files are distributed via GitHub Releases (Brotli-compressed GeoJSON)
POSTAL_CODE_GEOJSON_URL = (
    "https://github.com/yetzt/postleitzahlen/releases/download/2024.12/postleitzahlen.geojson.br"
)


def download_postal_codes() -> Dict[str, Any]:
    """
    Download postal code GeoJSON from GitHub releases.

    The file is Brotli-compressed (.br), so we decompress it before parsing.

    Returns:
        GeoJSON FeatureCollection

    Raises:
        Exception: If download fails
    """
    logger.info(f"Downloading postal codes from {POSTAL_CODE_GEOJSON_URL}")

    try:
        with urlopen(POSTAL_CODE_GEOJSON_URL, timeout=60) as response:
            # Decompress Brotli data
            compressed_data = response.read()
            decompressed_data = brotli.decompress(compressed_data)
            data = json.loads(decompressed_data.decode("utf-8"))
            logger.info(f"Downloaded {len(data.get('features', []))} postal code features")
            return data
    except Exception as e:
        logger.error(f"Failed to download postal codes: {e}")
        raise


def filter_postal_codes(geojson: Dict[str, Any], prefix: str = None) -> List[Dict[str, Any]]:
    """
    Filter postal codes by prefix.

    Args:
        geojson: GeoJSON FeatureCollection
        prefix: Postal code prefix (e.g., "10" for Berlin)

    Returns:
        List of filtered features
    """
    features = geojson.get("features", [])

    if not prefix:
        return features

    # Try both property names: 'plz' (yetzt/postleitzahlen) and 'plz_code' (opendatasoft)
    filtered = [
        f
        for f in features
        if f.get("properties", {}).get("plz", f.get("properties", {}).get("plz_code", "")).startswith(prefix)
    ]

    logger.info(
        f"Filtered {len(filtered)} postal codes with prefix '{prefix}' "
        f"from {len(features)} total"
    )
    return filtered


def insert_postal_codes(features: List[Dict[str, Any]]) -> int:
    """
    Insert postal code geometries into PostGIS.

    Args:
        features: List of GeoJSON features

    Returns:
        Number of postal codes inserted
    """
    logger.info(f"Inserting {len(features)} postal codes into database")

    inserted_count = 0

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            for feature in features:
                try:
                    properties = feature.get("properties", {})
                    geometry = feature.get("geometry", {})

                    # Try both property names: 'plz' (yetzt/postleitzahlen) and 'plz_code' (opendatasoft)
                    postal_code = properties.get("plz") or properties.get("plz_code")

                    if not postal_code or not geometry:
                        logger.warning(f"Skipping invalid feature: {feature}")
                        continue

                    # Convert GeoJSON geometry to WKT for PostGIS
                    geojson_str = json.dumps(geometry)

                    query = """
                        INSERT INTO postal_codes (postal_code, geometry)
                        VALUES (%s, ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326))
                        ON CONFLICT (postal_code) DO UPDATE
                        SET geometry = EXCLUDED.geometry,
                            updated_at = CURRENT_TIMESTAMP
                    """

                    cur.execute(query, (postal_code, geojson_str))
                    inserted_count += 1

                    if inserted_count % 100 == 0:
                        logger.info(f"Processed {inserted_count} postal codes...")

                except Exception as e:
                    logger.error(f"Error inserting postal code {postal_code}: {e}")
                    continue

    logger.info(f"Successfully inserted/updated {inserted_count} postal codes")
    return inserted_count


def create_spatial_indexes() -> None:
    """Create spatial indexes for performance."""
    logger.info("Creating/refreshing spatial indexes...")

    queries = [
        "CREATE INDEX IF NOT EXISTS idx_postal_codes_geom ON postal_codes USING GIST (geometry);",
        "CREATE INDEX IF NOT EXISTS idx_postal_codes_centroid ON postal_codes USING GIST (centroid);",
        "ANALYZE postal_codes;",
    ]

    for query in queries:
        try:
            execute_query(query)
        except Exception as e:
            logger.warning(f"Index creation warning: {e}")


def main() -> None:
    """Main ingestion workflow."""
    logger.info("Starting postal code ingestion")

    try:
        # Download GeoJSON data
        geojson = download_postal_codes()

        # Filter by prefix if configured
        features = filter_postal_codes(geojson, config.app.postal_code_filter)

        if not features:
            logger.warning("No postal codes to insert after filtering")
            return

        # Insert into database
        count = insert_postal_codes(features)

        # Create indexes
        create_spatial_indexes()

        logger.info(f"✓ Postal code ingestion completed: {count} postal codes")

    except Exception as e:
        logger.error(f"✗ Postal code ingestion failed: {e}")
        raise


if __name__ == "__main__":
    main()
