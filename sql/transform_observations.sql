-- Transform raw API responses into raw_weather_observations table
-- This script extracts observation weather records from JSONB

WITH unique_observations AS (
    SELECT DISTINCT ON (ws.id, (weather->>'timestamp')::TIMESTAMP, (weather->>'source_id')::INTEGER)
        ws.id as station_id,
        (weather->>'timestamp')::TIMESTAMP as timestamp,
        (weather->>'source_id')::INTEGER as source_id,
        (weather->>'temperature')::FLOAT as temperature,
        (weather->>'precipitation')::FLOAT as precipitation,
        (weather->>'pressure_msl')::FLOAT as pressure_msl,
        (weather->>'relative_humidity')::FLOAT as relative_humidity,
        (weather->>'wind_speed')::FLOAT as wind_speed,
        (weather->>'wind_direction')::INTEGER as wind_direction,
        (weather->>'wind_gust_speed')::FLOAT as wind_gust_speed,
        (weather->>'wind_gust_direction')::INTEGER as wind_gust_direction,
        (weather->>'cloud_cover')::INTEGER as cloud_cover,
        (weather->>'visibility')::FLOAT as visibility,
        (weather->>'sunshine')::FLOAT as sunshine,
        (weather->>'solar')::FLOAT as solar,
        (weather->>'dew_point')::FLOAT as dew_point,
        weather->>'condition' as condition,
        weather->>'icon' as icon,
        (weather->>'precipitation_probability')::INTEGER as precipitation_probability,
        (weather->>'precipitation_probability_6h')::INTEGER as precipitation_probability_6h
    FROM raw_api_responses r,
         LATERAL jsonb_array_elements(r.response_json->'weather') as weather
    JOIN weather_stations ws ON ws.id = (weather->>'source_id')::INTEGER
    WHERE NOT r.processed
      AND ws.observation_type IN ('current', 'synop')
    ORDER BY ws.id, (weather->>'timestamp')::TIMESTAMP, (weather->>'source_id')::INTEGER, r.fetched_at DESC
)
INSERT INTO raw_weather_observations (
    station_id,
    timestamp,
    source_id,
    temperature,
    precipitation,
    pressure_msl,
    relative_humidity,
    wind_speed,
    wind_direction,
    wind_gust_speed,
    wind_gust_direction,
    cloud_cover,
    visibility,
    sunshine,
    solar,
    dew_point,
    condition,
    icon,
    precipitation_probability,
    precipitation_probability_6h
)
SELECT
    station_id,
    timestamp,
    source_id,
    temperature,
    precipitation,
    pressure_msl,
    relative_humidity,
    wind_speed,
    wind_direction,
    wind_gust_speed,
    wind_gust_direction,
    cloud_cover,
    visibility,
    sunshine,
    solar,
    dew_point,
    condition,
    icon,
    precipitation_probability,
    precipitation_probability_6h
FROM unique_observations
ON CONFLICT (station_id, timestamp, source_id) DO NOTHING;
