CREATE SCHEMA IF NOT EXISTS raw_data;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

SET search_path TO raw_data, staging, analytics, public;

-- Recently played Tracks
CREATE TABLE IF NOT EXISTS raw_data.recently_played (
    id SERIAL PRIMARY KEY,
    played_at TIMESTAMP WITH TIME ZONE,
    track_id VARCHAR(50),
    track_name TEXT,
    artist_ids TEXT,
    artist_names TEXT,
    album_id VARCHAR(50),
    album_name TEXT,
    duration_ms INTEGER,
    popularity INTEGER,
    explicit BOOLEAN,
    extracted_at TIMESTAMP WITH TIME ZONE,
    load_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Top Tracks
CREATE TABLE IF NOT EXISTS raw_data.top_tracks (
    id SERIAL PRIMARY KEY,
    rank INTEGER,
    time_range VARCHAR(20),
    track_id VARCHAR(50),
    track_name TEXT,
    artist_ids TEXT,
    artist_names TEXT,
    album_id VARCHAR(50),
    album_name TEXT,
    duration_ms INTEGER,
    popularity INTEGER,
    explicit BOOLEAN,
    extracted_at TIMESTAMP WITH TIME ZONE,
    load_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS raw_data.top_artists (
    id SERIAL PRIMARY KEY,
    rank INTEGER,
    time_range VARCHAR(20),
    artist_id VARCHAR(50),
    artist_name TEXT,
    genres TEXT,
    popularity INTEGER,
    followers INTEGER,
    extracted_at TIMESTAMP WITH TIME ZONE,
    load_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);


-- Create indexes for better performance
CREATE INDEX idx_recently_played_track_id ON raw_data.recently_played(track_id);
CREATE INDEX idx_recently_played_played_at ON raw_data.recently_played(played_at);
CREATE INDEX idx_top_tracks_track_id ON raw_data.top_tracks(track_id);
CREATE INDEX idx_top_artists_artist_id ON raw_data.top_artists(artist_id);

-- Create staging tables (cleaned versions)
CREATE TABLE IF NOT EXISTS staging.tracks (
    track_id VARCHAR(50) PRIMARY KEY,
    track_name TEXT,
    album_id VARCHAR(50),
    album_name TEXT,
    duration_ms INTEGER,
    popularity INTEGER,
    explicit BOOLEAN,
    first_seen_at TIMESTAMP WITH TIME ZONE,
    last_updated_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS staging.artists (
    artist_id VARCHAR(50) PRIMARY KEY,
    artist_name TEXT,
    genres TEXT[],
    popularity INTEGER,
    followers INTEGER,
    first_seen_at TIMESTAMP WITH TIME ZONE,
    last_updated_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS staging.track_artists (
    track_id VARCHAR(50),
    artist_id VARCHAR(50),
    artist_position INTEGER,
    PRIMARY KEY (track_id, artist_id)
);

-- Analytics tables (will be populated by dbt later)
CREATE TABLE IF NOT EXISTS analytics.fact_plays (
    play_id SERIAL PRIMARY KEY,
    played_at TIMESTAMP WITH TIME ZONE,
    track_id VARCHAR(50),
    user_listening_session_id INTEGER,
    day_of_week INTEGER,
    hour_of_day INTEGER
);

CREATE TABLE IF NOT EXISTS analytics.dim_tracks (
    track_id VARCHAR(50) PRIMARY KEY,
    track_name TEXT,
    album_name TEXT,
    primary_artist_name TEXT,
    duration_seconds DECIMAL(10,2),
    popularity_score INTEGER,
    is_explicit BOOLEAN
);

