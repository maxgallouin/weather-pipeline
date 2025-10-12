-- Refresh precomputed postal code to weather station distances
-- Computes top 3 nearest stations for each postal code using inverse distance squared weighting

-- Clear existing data
TRUNCATE TABLE postal_code_nearest_stations;

-- Recompute for observations (current + synop stations)
WITH ranked_distances AS (
    SELECT
        pc.postal_code,
        ws.id as station_id,
        ws.observation_type,
        ST_Distance(ws.location::geography, pc.centroid::geography) / 1000.0 as distance_km,
        1.0 / NULLIF(POWER(ST_Distance(ws.location::geography, pc.centroid::geography) / 1000.0, 2), 0) as weight,
        ROW_NUMBER() OVER (PARTITION BY pc.postal_code, ws.observation_type ORDER BY ws.location <-> pc.centroid) as rank
    FROM postal_codes pc
    CROSS JOIN weather_stations ws
    WHERE ws.observation_type IN ('current', 'synop')
)
INSERT INTO postal_code_nearest_stations (postal_code, station_id, observation_type, distance_km, weight, rank)
SELECT postal_code, station_id, observation_type, distance_km, weight, rank
FROM ranked_distances
WHERE rank <= 3;

-- Recompute for forecasts
WITH ranked_distances AS (
    SELECT
        pc.postal_code,
        ws.id as station_id,
        ws.observation_type,
        ST_Distance(ws.location::geography, pc.centroid::geography) / 1000.0 as distance_km,
        1.0 / NULLIF(POWER(ST_Distance(ws.location::geography, pc.centroid::geography) / 1000.0, 2), 0) as weight,
        ROW_NUMBER() OVER (PARTITION BY pc.postal_code ORDER BY ws.location <-> pc.centroid) as rank
    FROM postal_codes pc
    CROSS JOIN weather_stations ws
    WHERE ws.observation_type = 'forecast'
)
INSERT INTO postal_code_nearest_stations (postal_code, station_id, observation_type, distance_km, weight, rank)
SELECT postal_code, station_id, observation_type, distance_km, weight, rank
FROM ranked_distances
WHERE rank <= 3;