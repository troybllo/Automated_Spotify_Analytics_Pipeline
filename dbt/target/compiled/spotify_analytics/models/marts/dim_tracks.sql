WITH tracks AS (
    SELECT * FROM "spotify_analytics"."analytics_staging"."tracks"
),

track_artists AS (
    SELECT DISTINCT
        track_id,
        SPLIT_PART(artist_names, ',', 1) as primary_artist_name
    FROM "spotify_analytics"."analytics_staging"."recently_played"
)

SELECT t.track_id,
    t.track_name,
    t.album_id,
    t.album_name,
    ta.primary_artist_name,
    t.duration_minutes,
    t.popularity as popularity_score,
    t.explicit as is_explicit,
    t.last_updated
FROM tracks t
LEFT JOIN track_artists ta ON t.track_id = ta.track_id