from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "spotify-analytics",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

dag = DAG(
    "spotify_weekly_analysis",
    default_args=default_args,
    description="Weekly analysis and aggregation of Spotify data",
    schedule_interval="@weekly",  # Run every Sunday at midnight
    catchup=False,
    tags=["spotify", "analysis"],
)

# Create summary tables
create_weekly_summary = PostgresOperator(
    task_id="create_weekly_summary",
    postgres_conn_id="spotify_postgres",
    sql="""
    -- Create or replace weekly listening summary
    CREATE TABLE IF NOT EXISTS analytics.weekly_listening_summary AS
    WITH weekly_stats AS (
        SELECT 
            DATE_TRUNC('week', played_at) as week,
            track_id,
            track_name,
            artist_names,
            COUNT(*) as play_count,
            SUM(duration_ms) / 1000.0 / 60 as total_minutes
        FROM raw_data.recently_played
        WHERE played_at >= CURRENT_DATE - INTERVAL '7 days'
        GROUP BY 1, 2, 3, 4
    )
    SELECT 
        week,
        track_id,
        track_name,
        artist_names,
        play_count,
        ROUND(total_minutes::numeric, 2) as total_minutes,
        RANK() OVER (PARTITION BY week ORDER BY play_count DESC) as weekly_rank
    FROM weekly_stats;
    
    -- Update if exists
    TRUNCATE analytics.weekly_listening_summary;
    INSERT INTO analytics.weekly_listening_summary
    SELECT * FROM (
        WITH weekly_stats AS (
            SELECT 
                DATE_TRUNC('week', played_at) as week,
                track_id,
                track_name,
                artist_names,
                COUNT(*) as play_count,
                SUM(duration_ms) / 1000.0 / 60 as total_minutes
            FROM raw_data.recently_played
            WHERE played_at >= CURRENT_DATE - INTERVAL '7 days'
            GROUP BY 1, 2, 3, 4
        )
        SELECT 
            week,
            track_id,
            track_name,
            artist_names,
            play_count,
            ROUND(total_minutes::numeric, 2) as total_minutes,
            RANK() OVER (PARTITION BY week ORDER BY play_count DESC) as weekly_rank
        FROM weekly_stats
    ) latest_week;
    """,
    dag=dag,
)

# Generate listening patterns
analyze_patterns = PostgresOperator(
    task_id="analyze_listening_patterns",
    postgres_conn_id="spotify_postgres",
    sql="""
    -- Create listening patterns table
    CREATE TABLE IF NOT EXISTS analytics.listening_patterns AS
    SELECT 
        EXTRACT(HOUR FROM played_at) as hour_of_day,
        EXTRACT(DOW FROM played_at) as day_of_week,
        CASE 
            WHEN EXTRACT(DOW FROM played_at) IN (0, 6) THEN 'Weekend'
            ELSE 'Weekday'
        END as day_type,
        COUNT(*) as play_count,
        COUNT(DISTINCT track_id) as unique_tracks,
        ROUND(AVG(duration_ms / 1000.0 / 60)::numeric, 2) as avg_track_length_minutes
    FROM raw_data.recently_played
    WHERE played_at >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY 1, 2, 3;
    
    -- Refresh the data
    TRUNCATE analytics.listening_patterns;
    INSERT INTO analytics.listening_patterns
    SELECT 
        EXTRACT(HOUR FROM played_at) as hour_of_day,
        EXTRACT(DOW FROM played_at) as day_of_week,
        CASE 
            WHEN EXTRACT(DOW FROM played_at) IN (0, 6) THEN 'Weekend'
            ELSE 'Weekday'
        END as day_type,
        COUNT(*) as play_count,
        COUNT(DISTINCT track_id) as unique_tracks,
        ROUND(AVG(duration_ms / 1000.0 / 60)::numeric, 2) as avg_track_length_minutes
    FROM raw_data.recently_played
    WHERE played_at >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY 1, 2, 3;
    """,
)

create_weekly_summary >> analyze_patterns
