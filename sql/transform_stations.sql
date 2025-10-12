-- Transform raw API responses into weather_stations table
-- This script extracts station metadata from JSONB and upserts into weather_stations

WITH unique_stations AS (
    SELECT DISTINCT ON ((source->>'id')::INTEGER)
        (source->>'id')::INTEGER as id,
        NULLIF(source->>'dwd_station_id', '') as dwd_station_id,
        source->>'station_name' as station_name,
        source->>'observation_type' as observation_type,
        ST_SetSRID(ST_MakePoint(
            (source->>'lon')::FLOAT,
            (source->>'lat')::FLOAT
        ), 4326) as location,
        (source->>'height')::FLOAT as height,
        source->>'wmo_station_id' as wmo_station_id,
        (source->>'first_record')::TIMESTAMP as first_record,
        (source->>'last_record')::TIMESTAMP as last_record
    FROM raw_api_responses r,
         LATERAL jsonb_array_elements(r.response_json->'sources') as source
    WHERE NOT r.processed
      AND source->>'id' IS NOT NULL
    ORDER BY (source->>'id')::INTEGER, r.fetched_at DESC
)
INSERT INTO weather_stations (
    id,
    dwd_station_id,
    station_name,
    observation_type,
    location,
    height,
    wmo_station_id,
    first_record,
    last_record
)
SELECT
    id,
    dwd_station_id,
    station_name,
    observation_type,
    location,
    height,
    wmo_station_id,
    first_record,
    last_record
FROM unique_stations
ON CONFLICT (id) DO UPDATE SET
    dwd_station_id = EXCLUDED.dwd_station_id,
    station_name = EXCLUDED.station_name,
    observation_type = EXCLUDED.observation_type,
    location = EXCLUDED.location,
    height = EXCLUDED.height,
    last_record = EXCLUDED.last_record,
    updated_at = CURRENT_TIMESTAMP;
