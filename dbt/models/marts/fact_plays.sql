WITH plays AS (
    SELECT * FROM {{ ref('recently_played') }}
),

enriched AS (
    SELECT
        p.play_id,
        p.played_at,
        p.track_id,
        DATE(p.played_at) as date_key,
        EXTRACT(HOUR FROM p.played_at) as hour_of_day,
        EXTRACT(DOW FROM p.played_at) as day_of_week,
        CASE 
            WHEN EXTRACT(DOW FROM p.played_at) IN (0, 6) THEN TRUE 
            ELSE FALSE 
        END as is_weekend,
        CASE 
            WHEN EXTRACT(HOUR FROM p.played_at) BETWEEN 6 AND 11 THEN 'Morning'
            WHEN EXTRACT(HOUR FROM p.played_at) BETWEEN 12 AND 17 THEN 'Afternoon'
            WHEN EXTRACT(HOUR FROM p.played_at) BETWEEN 18 AND 22 THEN 'Evening'
            ELSE 'Night'
        END as time_of_day,
        p.duration_minutes,
        p.explicit as is_explicit,
        p.load_timestamp
    FROM plays p
)

SELECT * FROM enriched
