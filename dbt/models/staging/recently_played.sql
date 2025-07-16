WITH source AS (
    SELECT * FROM {{ source('spotify_raw', 'recently_played') }}
),

deduplicated AS (
    SELECT DISTINCT
        played_at,
        track_id,
        track_name,
        artist_ids,
        artist_names,
        album_id,
        album_name,
        duration_ms,
        popularity,
        explicit,
        extracted_at,
        load_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY played_at, track_id 
            ORDER BY load_timestamp DESC
        ) as rn
    FROM source
    WHERE track_id IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['played_at', 'track_id']) }} as play_id,
    played_at,
    track_id,
    track_name,
    artist_ids,
    artist_names,
    album_id,
    album_name,
    duration_ms,
    ROUND(duration_ms / 1000.0 / 60, 2) as duration_minutes,
    popularity,
    explicit,
    extracted_at,
    load_timestamp
FROM deduplicated
WHERE rn = 1
