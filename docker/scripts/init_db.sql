-- docker/scripts/init_db.sql
-- PostgreSQL init script run once on first container startup via /docker-entrypoint-initdb.d/
-- Creates schemas and stub tables so Airflow healthchecks pass before ingestion runs.

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

CREATE TABLE IF NOT EXISTS raw.fred_observations (
    series_id           TEXT             NOT NULL,
    observation_date    DATE             NOT NULL,
    value               DOUBLE PRECISION,
    value_is_missing    BOOLEAN          DEFAULT FALSE,
    description         TEXT,
    category            TEXT,
    units               TEXT,
    frequency           TEXT,
    source              TEXT             DEFAULT 'fred',
    series_url          TEXT,
    ingested_at         TIMESTAMPTZ      NOT NULL DEFAULT now(),
    pipeline_version    TEXT,
    _dlt_load_id        TEXT,
    _dlt_id             TEXT             PRIMARY KEY
);
CREATE INDEX IF NOT EXISTS idx_fred_obs_series_date ON raw.fred_observations (series_id, observation_date);
CREATE INDEX IF NOT EXISTS idx_fred_obs_ingested_at ON raw.fred_observations (ingested_at DESC);

CREATE TABLE IF NOT EXISTS raw.bea_nipa_observations (
    table_key            TEXT             NOT NULL,
    series_name          TEXT             NOT NULL,
    period_date          DATE             NOT NULL,
    value                DOUBLE PRECISION,
    is_missing           BOOLEAN          DEFAULT FALSE,
    bea_table_name       TEXT,
    bea_line_number      INTEGER,
    bea_line_description TEXT,
    bea_period_str       TEXT,
    frequency            TEXT,
    units                TEXT,
    description          TEXT,
    source               TEXT             DEFAULT 'bea',
    ingested_at          TIMESTAMPTZ      NOT NULL DEFAULT now(),
    pipeline_version     TEXT,
    _dlt_load_id         TEXT,
    _dlt_id              TEXT             PRIMARY KEY
);
CREATE INDEX IF NOT EXISTS idx_bea_obs_key_series_period ON raw.bea_nipa_observations (table_key, series_name, period_date);
CREATE INDEX IF NOT EXISTS idx_bea_obs_ingested_at ON raw.bea_nipa_observations (ingested_at DESC);



CREATE TABLE IF NOT EXISTS raw.bls_observations (
    series_id           TEXT             NOT NULL,
    observation_date    DATE             NOT NULL,
    value               DOUBLE PRECISION,
    is_missing          BOOLEAN          DEFAULT FALSE,
    year                INTEGER,
    period              TEXT,
    period_name         TEXT,
    description         TEXT,
    category            TEXT,
    units               TEXT,
    frequency           TEXT,
    source              TEXT             DEFAULT 'bls',
    series_url          TEXT,
    ingested_at         TIMESTAMPTZ      NOT NULL DEFAULT now(),
    pipeline_version    TEXT,
    _dlt_load_id        TEXT,
    _dlt_id             TEXT             PRIMARY KEY
);
CREATE INDEX IF NOT EXISTS idx_bls_obs_series_date ON raw.bls_observations (series_id, observation_date);
