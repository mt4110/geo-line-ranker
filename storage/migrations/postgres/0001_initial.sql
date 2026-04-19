CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS schools (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    area TEXT NOT NULL,
    school_type TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    school_id TEXT NOT NULL REFERENCES schools (id) ON DELETE CASCADE,
    title TEXT NOT NULL,
    is_open_day BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS stations (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    line_name TEXT NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    geom geography(Point, 4326) GENERATED ALWAYS AS (
        ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography
    ) STORED
);

CREATE INDEX IF NOT EXISTS stations_geom_idx ON stations USING GIST (geom);

CREATE TABLE IF NOT EXISTS school_station_links (
    school_id TEXT NOT NULL REFERENCES schools (id) ON DELETE CASCADE,
    station_id TEXT NOT NULL REFERENCES stations (id) ON DELETE CASCADE,
    walking_minutes SMALLINT NOT NULL,
    distance_meters INTEGER NOT NULL,
    hop_distance SMALLINT NOT NULL,
    line_name TEXT NOT NULL,
    PRIMARY KEY (school_id, station_id)
);

CREATE TABLE IF NOT EXISTS user_events (
    id BIGSERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    school_id TEXT REFERENCES schools (id) ON DELETE SET NULL,
    event_type TEXT NOT NULL,
    occurred_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS recommendation_traces (
    id BIGSERIAL PRIMARY KEY,
    request_payload JSONB NOT NULL,
    response_payload JSONB NOT NULL,
    fallback_stage TEXT NOT NULL,
    algorithm_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS worker_jobs (
    id BIGSERIAL PRIMARY KEY,
    job_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'queued',
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

