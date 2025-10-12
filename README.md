# Enpal Weather Data Pipeline

> Production-ready data ingestion and transformation pipeline for German weather data from DWD via BrightSky API. Transforms raw weather observations and forecasts into ML-ready hourly data aggregated by postal code.

## Table of Contents

- [Quick Start](#quick-start)
- [Overview](#overview)
- [Architecture](#architecture)
- [Technology Stack](#technology-stack)
- [Data Models](#data-models)
- [Setup & Configuration](#setup--configuration)
- [Usage](#usage)
- [Data Quality](#data-quality)
- [Production Considerations](#production-considerations)
- [Data Sources](#data-sources)

---

## Quick Start

```bash
# Start services
docker compose up -d

# Wait for initialization (~30 seconds)
docker compose ps

# Access Airflow UI
open http://localhost:8080
# Login: admin / admin

# Trigger initial setup
docker compose exec airflow-webserver airflow dags trigger postal_code_ingestion
docker compose exec airflow-webserver airflow dags trigger weather_stations_sync
```

Pipelines will then run automatically:
- **Postal codes**: Daily at 2 AM
- **Stations sync**: Every 6 hours
- **Observations**: Every 15 minutes
- **Forecasts**: Every hour

---

## Overview

This pipeline delivers **ML-ready weather data** in 1-hour temporal resolution aggregated to German postal code areas. It implements a robust ELT (Extract-Load-Transform) pattern with comprehensive data quality checks.

### Key Features

✅ **Near real-time data** (15-minute observations, hourly forecasts)
✅ **ML-ready output** (cleaned, validated, aggregated to 1h×postal_code)
✅ **Comprehensive quality checks** (6+ validation steps)
✅ **Geospatial aggregation** (inverse distance weighting with K=3 nearest stations)
✅ **Production-ready** (containerized, orchestrated, monitored)
✅ **Scalable** (easily expandable from Berlin to full Germany)

---

## Architecture

### System Overview

```
┌─────────────────────────────────────────────────────────┐
│  Data Sources                                           │
│  • BrightSky API (DWD weather data)                     │
│  • yetzt/postleitzahlen (OSM postal code boundaries)    │
└────────────────┬────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────┐
│  Extract & Load (ELT Pattern)                           │
│  • Raw JSON → JSONB storage (reprocessability)          │
│  • SQL-based extraction → structured tables             │
│  • Parallel ingestion (5 workers)                       │
└────────────────┬────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────┐
│  Raw Storage (PostgreSQL + PostGIS)                     │
│  • raw_api_responses (JSONB)                            │
│  • postal_codes, weather_stations                       │
│  • raw_weather_observations/forecasts                   │
└────────────────┬────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────┐
│  Transform                                              │
│  • Spatial aggregation (IDW with K=3 stations)          │
│  • Data validation & outlier detection                  │
│  • Quality scoring & feature engineering                │
└────────────────┬────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────┐
│  ML-Ready Storage                                       │
│  • weather_observations_by_postal_code                  │
│  • weather_forecasts_by_postal_code                     │
│  Format: 1h resolution × postal code                    │
└────────────────┬────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────┐
│  Downstream ML Models                                   │
│  • Energy demand forecasting                            │
│  • Solar production prediction                          │
└─────────────────────────────────────────────────────────┘
```

### Pipeline Architecture (4 DAGs)

| Pipeline | Schedule | Purpose |
|----------|----------|---------|
| **postal_code_ingestion** | Daily 2 AM | Sync postal code boundaries from GitHub |
| **weather_stations_sync** | Every 6 hours | Refresh station metadata & distance matrix |
| **weather_observations_pipeline** | Every 15 min | Near real-time observations (today only) |
| **weather_forecasts_pipeline** | Every hour | Latest forecasts (configurable horizon) |

**Why separate DAGs?**
- **Separation of concerns**: Each DAG has single responsibility
- **Optimal scheduling**: Match data source update frequencies
- **Cost optimization**: Avoid redundant station syncs (~97% reduction)
- **Data freshness**: Near real-time availability without excessive API load

---

## Technology Stack

### Core Technologies

**PostgreSQL 15 + PostGIS**
- Native geospatial operations (ST_Distance, ST_Centroid, ST_Contains)
- ACID transactions for data consistency
- Excellent for complex spatial joins
- *Alternative considered*: TimescaleDB (better for pure time-series, but weaker spatial support)

**Apache Airflow 2.10.2**
- Industry-standard orchestration with DAG visualization
- Built-in monitoring, retry logic, and alerting
- TaskFlow API for clean Python code
- *Alternative considered*: Prefect (more modern but less mature ecosystem)

**Docker Compose**
- Reproducible environments (dev = prod)
- Easy setup and teardown
- Service isolation
- *Production path*: Kubernetes with Helm charts for scaling

**Python 3.11**
- 10-25% faster than 3.10
- Excellent data engineering ecosystem
- Type hints for code quality

### Key Design Decisions

✅ **ELT over ETL**: Store raw JSON first → enables reprocessing without re-fetching
✅ **Three-layer architecture**: Raw JSON → Structured raw → ML-ready transformed
✅ **Inverse distance weighting**: Physically intuitive, computationally efficient (K=3 nearest stations)
✅ **Query-time weight normalization**: Handles missing station data gracefully
✅ **Soft validation**: Detect outliers but don't remove (ML can filter by quality_score)

---

## Data Models

### Raw Layer

```sql
-- Complete API responses (reprocessability)
raw_api_responses (postal_code, date, endpoint, response_json::JSONB, fetched_at)

-- Geospatial data
postal_codes (postal_code, geometry, centroid, area_km2)  -- PostGIS GEOMETRY
weather_stations (id, dwd_station_id, location, observation_type)

-- Precomputed distances (updated every 6 hours)
postal_code_nearest_stations (postal_code, station_id, distance_km, weight, rank)
    -- K=3 nearest stations per postal code
    -- weight = 1 / distance² (inverse distance weighting)

-- Structured raw data (extracted from JSONB)
raw_weather_observations (station_id, timestamp, temperature, precipitation, ...)
raw_weather_forecasts (station_id, timestamp, forecast_created_at, ...)
```

### ML-Ready Layer (1h × postal_code)

```sql
weather_observations_by_postal_code (
    postal_code,
    timestamp,  -- Hourly resolution
    temperature, precipitation, pressure_msl, relative_humidity,
    wind_speed, wind_direction, cloud_cover, visibility,
    temperature_feels_like,  -- Derived: heat index / wind chill
    precipitation_intensity,  -- Derived: none/light/moderate/heavy
    weather_condition,
    -- Quality metadata
    num_stations_used,       -- How many stations contributed
    avg_station_distance_km,
    data_completeness,       -- % of non-null core fields
    quality_score,           -- Composite quality (0-1)
    has_outliers,            -- Boolean flag
    processed_at
)

weather_forecasts_by_postal_code (
    postal_code,
    timestamp,               -- Forecast valid time (hourly)
    forecast_created_at,     -- When forecast was generated
    forecast_horizon_hours,  -- Hours ahead
    ... same weather fields as observations ...,
    precipitation_probability  -- Unique to forecasts
)
```

---

## Setup & Configuration

### Prerequisites

- Docker & Docker Compose
- 4GB RAM available
- 10GB disk space

### Configuration (.env)

```bash
# Spatial scope (empty = all Germany, or "10" for Berlin area)
POSTAL_CODE_FILTER=

# Forecast horizon (1 for testing, 10 for production)
FORECAST_HORIZON_DAYS=1

# Database
POSTGRES_USER=weather_user
POSTGRES_PASSWORD=weather_pass
POSTGRES_DB=weather_db

# Airflow
AIRFLOW_USER=admin
AIRFLOW_PASSWORD=admin
```

### Project Structure

```
enpal-weather-pipeline/
├── docker-compose.yml          # Container orchestration
├── Dockerfile                  # Python app container
├── pyproject.toml             # Poetry dependencies
├── .env                       # Configuration
├── README.md                  # This file
├── dags/                      # Airflow DAGs
│   ├── postal_code_ingestion.py
│   ├── weather_stations_sync.py
│   ├── weather_observations_pipeline.py
│   └── weather_forecasts_pipeline.py
├── src/                       # Application code
│   ├── config.py              # Configuration management
│   ├── database.py            # Database operations
│   ├── ingest_postal_codes.py # Postal code ingestion
│   ├── ingest_weather_raw.py  # Weather API ingestion
│   ├── transform_weather.py   # Transformation & validation
│   └── data_quality_checks.py # Post-transformation checks
├── sql/                       # SQL scripts
│   ├── init-databases.sql     # DB initialization
│   ├── schema.sql             # Complete schema
│   ├── transform_*.sql        # Transformation queries
│   └── refresh_nearest_stations.sql  # Distance matrix
└── tests/                     # Unit tests
```

---

## Usage

### Running Pipelines

**Airflow UI** (recommended):
1. Open http://localhost:8080
2. Enable DAGs in the UI
3. Trigger manually or wait for scheduled runs

**CLI**:
```bash
# Trigger DAGs manually
docker compose exec airflow-webserver airflow dags trigger postal_code_ingestion
docker compose exec airflow-webserver airflow dags trigger weather_stations_sync
docker compose exec airflow-webserver airflow dags trigger weather_observations_pipeline
docker compose exec airflow-webserver airflow dags trigger weather_forecasts_pipeline

# Check status
docker compose exec airflow-webserver airflow dags list
docker compose exec airflow-webserver airflow dags list-runs -d weather_observations_pipeline
```

### Querying Data

```bash
# Connect to database
docker-compose exec postgres psql -U weather_user -d weather_db
```

```sql
-- Latest observations by postal code
SELECT
    postal_code,
    timestamp,
    temperature,
    precipitation,
    wind_speed,
    quality_score,
    num_stations_used
FROM weather_observations_by_postal_code
WHERE timestamp > NOW() - INTERVAL '24 hours'
ORDER BY timestamp DESC
LIMIT 10;

-- Weather forecasts for next 24 hours
SELECT
    postal_code,
    timestamp AS forecast_valid_time,
    forecast_created_at,
    temperature,
    precipitation_probability,
    quality_score
FROM weather_forecasts_by_postal_code
WHERE timestamp BETWEEN NOW() AND NOW() + INTERVAL '24 hours'
ORDER BY postal_code, timestamp;

-- Data quality overview
SELECT
    COUNT(*) as total_records,
    AVG(quality_score) as avg_quality,
    COUNT(*) FILTER (WHERE has_outliers) as outlier_count,
    AVG(num_stations_used) as avg_stations
FROM weather_observations_by_postal_code
WHERE timestamp > NOW() - INTERVAL '24 hours';
```

---

## Data Quality

### Validation Steps (6+ checks)

**During Transformation** (`transform_weather.py`):

1. **Physical constraint validation**: Temperature [-50, 50]°C, precipitation [0, 300]mm/h, etc.
2. **Outlier detection**: IQR-based statistical detection with 30% quality penalty
3. **Null handling**: Quality scoring based on completeness
4. **Duplicate removal**: Database constraints on (station_id, timestamp)
5. **Consistency checks**: Cloud cover ≤ 100%, wind speed ≥ 0
6. **Quality scoring**: Composite score (completeness + outlier penalty + multi-station bonus)

**Post-Transformation** (`data_quality_checks.py`):

1. **Data freshness**: Age < 1h for observations, < 2h for forecasts
2. **Temporal continuity**: Detect gaps > 2h in time series
3. **Data completeness**: ≥80% non-null for core fields
4. **Quality score validation**: Avg quality ≥ 0.7, low-quality records < 20%
5. **Spatial coverage**: ≥5 postal codes have data
6. **Anomaly detection**: IQR method with 3×IQR threshold (statistical outliers)

### Quality Scoring Algorithm

```python
# Base score = data completeness (0-1)
score = non_null_fields / total_core_fields

# Penalty for outliers (soft validation - data kept)
if has_outliers:
    score *= 0.7  # 30% reduction

# Bonus for multiple stations (better spatial coverage)
if num_stations >= 3:
    score *= 1.1  # 10% boost
elif num_stations >= 2:
    score *= 1.05  # 5% boost

# Cap at 1.0
final_score = min(score, 1.0)
```

**ML Usage**: Filter by `quality_score >= 0.7` for high-quality data only.

### Spatial Aggregation (Inverse Distance Weighting)

**Formula**: `weight = 1 / distance²`

**Why weights DON'T sum to 1**:
- We normalize at **query time**, not storage time
- Handles missing station data gracefully
- Example: If Station C is offline, weights automatically renormalize

```sql
-- Query-time normalization (correct approach)
SUM(temperature * weight) / NULLIF(SUM(weight), 0)

-- If pre-normalized to 1 (incorrect):
-- Would lose weight when stations are missing!
```

**Concrete example**:
```
Station A: 5 km  → weight = 1/25 = 0.040
Station B: 10 km → weight = 1/100 = 0.010
Station C: 20 km → weight = 1/400 = 0.0025
SUM = 0.0525 (not 1.0!)

When all have data:
  temp = (12°C × 0.040 + 11°C × 0.010 + 10°C × 0.0025) / 0.0525
       = 11.71°C ✓

When Station C missing:
  temp = (12°C × 0.040 + 11°C × 0.010) / (0.040 + 0.010)
       = 11.80°C ✓ (automatically renormalized!)
```

---

## Production Considerations

### Scalability

**Current setup** (single node):
- Handles all 8,168 German postal codes
- ~3-5 min ingestion time
- LocalExecutor (parallel tasks on same machine)

**Further scaling** (higher throughput):
- Increase to 10-20 parallel workers
- Add CeleryExecutor + Redis for distributed processing
- Implement table partitioning by month
- Add read replicas for analytics queries

**Database optimization**:
```sql
-- Partition by month for historical data
CREATE TABLE weather_observations_by_postal_code_2025_01
    PARTITION OF weather_observations_by_postal_code
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');

-- Materialized views for common queries
CREATE MATERIALIZED VIEW latest_weather_by_postal_code AS
SELECT DISTINCT ON (postal_code)
    postal_code, timestamp, temperature, ...
FROM weather_observations_by_postal_code
ORDER BY postal_code, timestamp DESC;
```

### Monitoring

**Airflow built-in**:
- Task duration metrics
- Success/failure rates
- DAG run history

**Custom metrics** (recommend adding):
```python
from prometheus_client import Counter, Gauge

records_ingested = Counter('weather_records_ingested_total')
data_quality = Gauge('weather_data_quality_score')
data_freshness = Gauge('weather_data_age_hours')
```

**Alerts** (recommend adding):
- Data freshness > 2 hours
- Quality score < 0.7
- Ingestion failure rate > 5%
- API response time > 5s

### Security

**Current** (suitable for development):
- Environment variables for credentials
- Docker network isolation
- No sensitive data (public weather data)

**Production enhancements**:
- AWS Secrets Manager / HashiCorp Vault
- TLS for database connections
- IAM roles for service accounts
- Rotate credentials quarterly
- VPC with private subnets

### Cost Optimization

**Already implemented**:
- ✅ Database deduplication (ON CONFLICT DO NOTHING)
- ✅ Batch API requests (100 records/batch)
- ✅ Parallel workers (5x faster ingestion)
- ✅ Precomputed distances (no recalculation per query)

**Production enhancements**:
- Redis caching for API responses (15-min TTL) → 90% reduction in API calls
- Archive data > 1 year to S3 Glacier → 90% storage cost reduction
- Compress historical tables (columnar format)
- Incremental ingestion (only new timestamps)

---

## Data Sources

### BrightSky API

**Source**: https://brightsky.dev/
**Provider**: Wrapper for DWD (Deutscher Wetterdienst) open data
**Update frequency**:
- Observations: Every 30 minutes (DWD synop), polled every 10 minutes by BrightSky
- Forecasts: 4x daily at 00/06/12/18 UTC (DWD ICON-EU model)

**What we fetch**:
```
GET /weather?lat={lat}&lon={lon}&date={date}
```
Returns nearest weather stations within 50km radius with:
- Historical observations (temperature, precipitation, wind, etc.)
- Weather forecasts (up to 10 days ahead)
- Station metadata (location, elevation, type)

### German Postal Code Boundaries

**Source**: https://github.com/yetzt/postleitzahlen
**Provider**: OpenStreetMap data extracted via Overpass API
**Format**: GeoJSON with Brotli compression (.br)
**Update frequency**: Irregularly (boundaries rarely change)

**What we fetch**:
```
https://github.com/yetzt/postleitzahlen/releases/download/2024.12/postleitzahlen.geojson.br
```
Returns ~8,200 German postal code polygons with:
- Postal code (5-digit)
- Geometry (POLYGON in WGS84)
- Auto-calculated centroid (via PostGIS trigger)
