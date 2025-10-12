-- Create separate databases for Airflow and Weather application
CREATE DATABASE airflow_db;
CREATE DATABASE weather_db;

-- Grant permissions
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO weather_user;
GRANT ALL PRIVILEGES ON DATABASE weather_db TO weather_user;