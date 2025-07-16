
  
    

  create  table "spotify_analytics"."analytics_analytics"."listen_habits__dbt_tmp"
  
  
    as
  
  (
    WITH daily_stats AS (
    SELECT
        f.date_key,
        COUNT(DISTINCT f.track_id) as unique_tracks,
        COUNT(*) as total_plays,
        SUM(f.duration_minutes) as total_minutes,
        AVG(t.popularity_score) as avg_track_popularity,
        0.5 as happy_ratio,
        0.5 as avg_energy,
        0.5 as avg_danceability
    FROM "spotify_analytics"."analytics_analytics"."fact_plays" f
    JOIN "spotify_analytics"."analytics_analytics"."dim_tracks" t ON f.track_id = t.track_id
    GROUP BY f.date_key
)

SELECT
    ds.*,
    dt.day_name,
    dt.is_weekend,
    dt.month_name,
    dt.season,
    ROUND(total_minutes / 60.0, 2) as total_hours
FROM daily_stats ds
JOIN "spotify_analytics"."analytics_analytics"."dim_times" dt ON ds.date_key = dt.date_key
ORDER BY ds.date_key DESC
  );
  