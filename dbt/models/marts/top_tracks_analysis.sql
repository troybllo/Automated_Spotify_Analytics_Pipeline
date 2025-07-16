WITH track_plays AS (
    SELECT 
        track_id,
        track_name,
        artist_names,
        COUNT(*) as play_count,
        SUM(duration_minutes) as total_minutes_played,
        AVG(popularity) as avg_popularity,
        MAX(played_at) as last_played,
        MIN(played_at) as first_played
    FROM {{ ref('recently_played') }}
    GROUP BY track_id, track_name, artist_names
)

SELECT 
    *,
    ROUND(total_minutes_played / 60.0, 2) as total_hours_played,
    CURRENT_DATE - DATE(last_played) as days_since_last_play
FROM track_plays
ORDER BY play_count DESC
