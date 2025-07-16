WITH all_tracks AS (
    -- Get tracks from recently played
    SELECT DISTINCT
        track_id,
        track_name,
        album_id,
        album_name,
        duration_ms,
        popularity,
        explicit
    FROM "spotify_analytics"."analytics_staging"."recently_played"
    
    UNION
    
    -- Get tracks from top tracks
    SELECT DISTINCT
        track_id,
        track_name,
        album_id,
        album_name,
        duration_ms,
        popularity,
        explicit
    FROM "spotify_analytics"."raw_data"."top_tracks"
),

deduplicated AS (
    SELECT
        track_id,
        track_name,
        album_id,
        album_name,
        duration_ms,
        popularity,
        explicit,
        ROW_NUMBER() OVER (
            PARTITION BY track_id 
            ORDER BY popularity DESC NULLS LAST
        ) as rn
    FROM all_tracks
    WHERE track_id IS NOT NULL
)

SELECT
    track_id,
    track_name,
    album_id,
    album_name,
    duration_ms,
    ROUND(duration_ms / 1000.0 / 60, 2) as duration_minutes,
    popularity,
    explicit,
    CURRENT_TIMESTAMP as last_updated
FROM deduplicated
WHERE rn = 1