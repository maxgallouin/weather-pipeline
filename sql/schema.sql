-- Connect to weather_db
\c weather_db

-- Enable PostGIS extension for geospatial operations
CREATE EXTENSION IF NOT EXISTS postgis;

-- Raw API Responses Table
-- Stores unprocessed JSON responses from BrightSky API for full audit trail
CREATE TABLE IF NOT EXISTS raw_api_responses (
    id BIGSERIAL PRIMARY KEY,
    postal_code VARCHAR(5),
    lat FLOAT,
    lon FLOAT,
    date DATE,
    endpoint VARCHAR(50),  -- 'weather', 'current_weather'
    response_json JSONB NOT NULL,
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed BOOLEAN DEFAULT FALSE,

    -- Deduplication constraint
    UNIQUE(postal_code, date, endpoint, fetched_at)
);

CREATE INDEX idx_raw_responses_processed ON raw_api_responses(processed) WHERE NOT processed;
CREATE INDEX idx_raw_responses_date ON raw_api_responses(date);
CREATE INDEX idx_raw_responses_json ON raw_api_responses USING GIN (response_json);

-- Postal Codes Table
-- Stores geometric boundaries of German postal codes from OpenStreetMap
CREATE TABLE IF NOT EXISTS postal_codes (
    id SERIAL PRIMARY KEY,
    postal_code VARCHAR(5) NOT NULL UNIQUE,
    geometry GEOMETRY(MULTIPOLYGON, 4326) NOT NULL,
    centroid GEOMETRY(POINT, 4326),
    area_km2 FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_postal_codes_geom ON postal_codes USING GIST (geometry);
CREATE INDEX idx_postal_codes_centroid ON postal_codes USING GIST (centroid);
CREATE INDEX idx_postal_codes_code ON postal_codes (postal_code);

-- Weather Stations Table
-- Metadata about DWD weather stations and forecast points
-- Note: dwd_station_id can be NULL for virtual forecast points (e.g., MOSMIX model points)
CREATE TABLE IF NOT EXISTS weather_stations (
    id INTEGER PRIMARY KEY,
    dwd_station_id VARCHAR(10),  -- Can be NULL for forecast stations
    station_name VARCHAR(255),
    observation_type VARCHAR(50),  -- 'current', 'forecast', 'synop'
    location GEOMETRY(POINT, 4326) NOT NULL,
    height FLOAT,
    wmo_station_id VARCHAR(10),
    first_record TIMESTAMP,
    last_record TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_weather_stations_location ON weather_stations USING GIST (location);
-- Note: dwd_station_id is NOT unique - same physical station can have different IDs for forecast vs observations
CREATE INDEX idx_weather_stations_dwd_id ON weather_stations (dwd_station_id) WHERE dwd_station_id IS NOT NULL;
CREATE INDEX idx_weather_stations_type ON weather_stations (observation_type);

-- Raw Weather Observations Table
-- Unprocessed weather observations from DWD stations
CREATE TABLE IF NOT EXISTS raw_weather_observations (
    id BIGSERIAL PRIMARY KEY,
    station_id INTEGER REFERENCES weather_stations(id),
    timestamp TIMESTAMP NOT NULL,
    source_id INTEGER,

    -- Core weather measurements
    temperature FLOAT,  -- °C
    precipitation FLOAT,  -- mm
    pressure_msl FLOAT,  -- hPa
    relative_humidity FLOAT,  -- %
    wind_speed FLOAT,  -- km/h
    wind_direction INTEGER,  -- degrees
    wind_gust_speed FLOAT,  -- km/h
    wind_gust_direction INTEGER,  -- degrees
    cloud_cover INTEGER,  -- %
    visibility FLOAT,  -- meters
    sunshine FLOAT,  -- minutes
    solar FLOAT,  -- J/cm²
    dew_point FLOAT,  -- °C

    -- Metadata
    condition VARCHAR(50),
    icon VARCHAR(50),
    precipitation_probability INTEGER,  -- %
    precipitation_probability_6h INTEGER,  -- %

    -- Quality flags
    is_validated BOOLEAN DEFAULT FALSE,
    has_outliers BOOLEAN DEFAULT FALSE,
    quality_score FLOAT,

    -- Timestamps
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Ensure no duplicate records
    UNIQUE(station_id, timestamp, source_id)
);

CREATE INDEX idx_raw_obs_station_time ON raw_weather_observations (station_id, timestamp DESC);
CREATE INDEX idx_raw_obs_timestamp ON raw_weather_observations (timestamp DESC);
CREATE INDEX idx_raw_obs_ingested ON raw_weather_observations (ingested_at DESC);

-- Raw Weather Forecasts Table
-- Unprocessed weather forecasts from DWD stations
CREATE TABLE IF NOT EXISTS raw_weather_forecasts (
    id BIGSERIAL PRIMARY KEY,
    station_id INTEGER REFERENCES weather_stations(id),
    timestamp TIMESTAMP NOT NULL,
    source_id INTEGER,
    forecast_created_at TIMESTAMP NOT NULL,

    -- Core weather measurements (same as observations)
    temperature FLOAT,
    precipitation FLOAT,
    pressure_msl FLOAT,
    relative_humidity FLOAT,
    wind_speed FLOAT,
    wind_direction INTEGER,
    wind_gust_speed FLOAT,
    wind_gust_direction INTEGER,
    cloud_cover INTEGER,
    visibility FLOAT,
    sunshine FLOAT,
    solar FLOAT,
    dew_point FLOAT,

    -- Metadata
    condition VARCHAR(50),
    icon VARCHAR(50),
    precipitation_probability INTEGER,
    precipitation_probability_6h INTEGER,

    -- Quality flags
    is_validated BOOLEAN DEFAULT FALSE,
    has_outliers BOOLEAN DEFAULT FALSE,
    quality_score FLOAT,

    -- Timestamps
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Ensure no duplicate records
    UNIQUE(station_id, timestamp, forecast_created_at, source_id)
);

CREATE INDEX idx_raw_fcst_station_time ON raw_weather_forecasts (station_id, timestamp DESC);
CREATE INDEX idx_raw_fcst_timestamp ON raw_weather_forecasts (timestamp DESC);
CREATE INDEX idx_raw_fcst_created ON raw_weather_forecasts (forecast_created_at DESC);
CREATE INDEX idx_raw_fcst_ingested ON raw_weather_forecasts (ingested_at DESC);

-- Transformed Weather Observations by Postal Code
-- ML-ready cleaned and validated hourly observations aggregated to postal code level
CREATE TABLE IF NOT EXISTS weather_observations_by_postal_code (
    id BIGSERIAL PRIMARY KEY,
    postal_code VARCHAR(5) NOT NULL REFERENCES postal_codes(postal_code),
    timestamp TIMESTAMP NOT NULL,

    -- Aggregated weather measurements (spatial average weighted by distance)
    temperature FLOAT,
    precipitation FLOAT,
    pressure_msl FLOAT,
    relative_humidity FLOAT,
    wind_speed FLOAT,
    wind_direction INTEGER,
    wind_gust_speed FLOAT,
    cloud_cover INTEGER,
    visibility FLOAT,
    sunshine FLOAT,
    solar FLOAT,
    dew_point FLOAT,

    -- Derived features for ML
    temperature_feels_like FLOAT,  -- Calculated from temp, humidity, wind
    precipitation_intensity VARCHAR(20),  -- 'none', 'light', 'moderate', 'heavy'
    weather_condition VARCHAR(50),

    -- Data quality indicators
    num_stations_used INTEGER,  -- How many stations contributed to this record
    avg_station_distance_km FLOAT,  -- Average distance of contributing stations
    data_completeness FLOAT,  -- 0.0 to 1.0
    quality_score FLOAT,  -- Overall quality score
    has_imputed_values BOOLEAN DEFAULT FALSE,

    -- Timestamps
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(postal_code, timestamp)
);

CREATE INDEX idx_obs_postal_time ON weather_observations_by_postal_code (postal_code, timestamp DESC);
CREATE INDEX idx_obs_timestamp ON weather_observations_by_postal_code (timestamp DESC);
CREATE INDEX idx_obs_quality ON weather_observations_by_postal_code (quality_score DESC);

-- Transformed Weather Forecasts by Postal Code
-- ML-ready cleaned and validated hourly forecasts aggregated to postal code level
CREATE TABLE IF NOT EXISTS weather_forecasts_by_postal_code (
    id BIGSERIAL PRIMARY KEY,
    postal_code VARCHAR(5) NOT NULL REFERENCES postal_codes(postal_code),
    timestamp TIMESTAMP NOT NULL,
    forecast_created_at TIMESTAMP NOT NULL,
    forecast_horizon_hours INTEGER,  -- Hours ahead from forecast creation

    -- Aggregated weather measurements
    temperature FLOAT,
    precipitation FLOAT,
    pressure_msl FLOAT,
    relative_humidity FLOAT,
    wind_speed FLOAT,
    wind_direction INTEGER,
    wind_gust_speed FLOAT,
    cloud_cover INTEGER,
    visibility FLOAT,
    sunshine FLOAT,
    solar FLOAT,
    dew_point FLOAT,
    precipitation_probability INTEGER,

    -- Derived features
    temperature_feels_like FLOAT,
    precipitation_intensity VARCHAR(20),
    weather_condition VARCHAR(50),

    -- Data quality indicators
    num_stations_used INTEGER,
    avg_station_distance_km FLOAT,
    data_completeness FLOAT,
    quality_score FLOAT,
    has_imputed_values BOOLEAN DEFAULT FALSE,

    -- Timestamps
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(postal_code, timestamp, forecast_created_at)
);

CREATE INDEX idx_fcst_postal_time ON weather_forecasts_by_postal_code (postal_code, timestamp DESC);
CREATE INDEX idx_fcst_timestamp ON weather_forecasts_by_postal_code (timestamp DESC);
CREATE INDEX idx_fcst_created ON weather_forecasts_by_postal_code (forecast_created_at DESC);
CREATE INDEX idx_fcst_horizon ON weather_forecasts_by_postal_code (forecast_horizon_hours);

-- Precomputed Postal Code to Weather Station Distances
-- This table stores the nearest stations for each postal code to avoid expensive ST_Distance calculations
CREATE TABLE IF NOT EXISTS postal_code_nearest_stations (
    id BIGSERIAL PRIMARY KEY,
    postal_code VARCHAR(5) NOT NULL REFERENCES postal_codes(postal_code),
    station_id INTEGER NOT NULL REFERENCES weather_stations(id),
    observation_type VARCHAR(50) NOT NULL,  -- 'current', 'forecast', 'synop'
    distance_km FLOAT NOT NULL,
    weight FLOAT NOT NULL,  -- Normalized inverse distance squared weight (sum of weights per postal code = 1.0)
    rank INTEGER NOT NULL,  -- Rank by distance (1 = nearest, 2 = second nearest, etc.)

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(postal_code, station_id, observation_type)
);

-- Indexes for fast lookups
CREATE INDEX idx_pcns_postal_type ON postal_code_nearest_stations (postal_code, observation_type);
CREATE INDEX idx_pcns_station ON postal_code_nearest_stations (station_id);
CREATE INDEX idx_pcns_rank ON postal_code_nearest_stations (postal_code, observation_type, rank);

-- Create update trigger for postal_codes
CREATE OR REPLACE FUNCTION update_postal_code_metadata()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    NEW.centroid = ST_Centroid(NEW.geometry);
    NEW.area_km2 = ST_Area(NEW.geometry::geography) / 1000000.0;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_postal_code_metadata
    BEFORE INSERT OR UPDATE ON postal_codes
    FOR EACH ROW
    EXECUTE FUNCTION update_postal_code_metadata();

-- Create update trigger for weather_stations
CREATE OR REPLACE FUNCTION update_station_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_station_timestamp
    BEFORE UPDATE ON weather_stations
    FOR EACH ROW
    EXECUTE FUNCTION update_station_timestamp();

-- View: Latest weather observations for each postal code
CREATE OR REPLACE VIEW v_latest_weather_by_postal_code AS
SELECT DISTINCT ON (postal_code)
    postal_code,
    timestamp,
    temperature,
    precipitation,
    wind_speed,
    cloud_cover,
    weather_condition,
    quality_score
FROM weather_observations_by_postal_code
ORDER BY postal_code, timestamp DESC;

-- View: Weather stations with their nearest postal codes
CREATE OR REPLACE VIEW v_stations_with_postal_codes AS
SELECT
    ws.id,
    ws.dwd_station_id,
    ws.station_name,
    ws.observation_type,
    pc.postal_code AS nearest_postal_code,
    ST_Distance(ws.location::geography, pc.centroid::geography) / 1000.0 AS distance_km
FROM weather_stations ws
CROSS JOIN LATERAL (
    SELECT postal_code, centroid
    FROM postal_codes
    ORDER BY ws.location <-> centroid
    LIMIT 1
) pc;

COMMENT ON TABLE postal_codes IS 'German postal code boundaries from OpenStreetMap';
COMMENT ON TABLE weather_stations IS 'DWD weather station metadata';
COMMENT ON TABLE raw_weather_observations IS 'Unprocessed weather observations from BrightSky API';
COMMENT ON TABLE raw_weather_forecasts IS 'Unprocessed weather forecasts from BrightSky API';
COMMENT ON TABLE weather_observations_by_postal_code IS 'ML-ready cleaned hourly weather observations per postal code';
COMMENT ON TABLE weather_forecasts_by_postal_code IS 'ML-ready cleaned hourly weather forecasts per postal code';
COMMENT ON TABLE postal_code_nearest_stations IS 'Precomputed distances from postal codes to their nearest weather stations to avoid expensive ST_Distance calculations during queries';