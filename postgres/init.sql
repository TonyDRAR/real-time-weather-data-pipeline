CREATE TABLE IF NOT EXISTS weather_observations (
    id BIGSERIAL PRIMARY KEY,
    event_id TEXT NOT NULL,
    city TEXT NOT NULL,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    observed_at TIMESTAMP WITHOUT TIME ZONE,
    ingested_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    processed_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    temperature_2m DOUBLE PRECISION,
    relative_humidity_2m INTEGER,
    apparent_temperature DOUBLE PRECISION,
    precipitation DOUBLE PRECISION,
    weather_code INTEGER,
    wind_speed_10m DOUBLE PRECISION,
    kafka_partition INTEGER,
    kafka_offset BIGINT
);

CREATE INDEX IF NOT EXISTS idx_weather_event_id
    ON weather_observations (event_id);

CREATE INDEX IF NOT EXISTS idx_weather_city_ingested_at
    ON weather_observations (city, ingested_at DESC);

CREATE INDEX IF NOT EXISTS idx_weather_observed_at
    ON weather_observations (observed_at DESC);

CREATE OR REPLACE VIEW weather_observations_dedup AS
SELECT
    id,
    event_id,
    city,
    latitude,
    longitude,
    observed_at,
    ingested_at,
    processed_at,
    temperature_2m,
    relative_humidity_2m,
    apparent_temperature,
    precipitation,
    weather_code,
    wind_speed_10m,
    kafka_partition,
    kafka_offset
FROM (
    SELECT
        observations.*,
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY processed_at DESC, id DESC
        ) AS row_number
    FROM weather_observations AS observations
) AS ranked
WHERE row_number = 1;

CREATE OR REPLACE VIEW weather_latest AS
SELECT DISTINCT ON (city)
    city,
    latitude,
    longitude,
    observed_at,
    ingested_at,
    processed_at,
    temperature_2m,
    relative_humidity_2m,
    apparent_temperature,
    precipitation,
    weather_code,
    wind_speed_10m
FROM weather_observations_dedup
ORDER BY city, ingested_at DESC;

CREATE OR REPLACE VIEW weather_hourly AS
SELECT
    city,
    DATE_TRUNC('hour', ingested_at) AS hour,
    AVG(temperature_2m) AS avg_temperature_2m,
    MIN(temperature_2m) AS min_temperature_2m,
    MAX(temperature_2m) AS max_temperature_2m,
    AVG(relative_humidity_2m) AS avg_relative_humidity_2m,
    SUM(COALESCE(precipitation, 0)) AS total_precipitation,
    MAX(wind_speed_10m) AS max_wind_speed_10m,
    COUNT(*) AS observation_count
FROM weather_observations_dedup
GROUP BY city, DATE_TRUNC('hour', ingested_at);
