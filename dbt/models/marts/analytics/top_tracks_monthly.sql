WITH monthly_plays AS (
    SELECT
        DATE_TRUNC('month', f.played_at) as month,
        f.track_id,
        t.track_name,
        t.primary_artist_name,
        COUNT(*) as play_count,
        SUM(f.duration_minutes) as total_minutes
    FROM {{ ref('fact_plays') }} f
    JOIN {{ ref('dim_tracks') }} t ON f.track_id = t.track_id
    GROUP BY 1, 2, 3, 4
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY month ORDER BY play_count DESC) as monthly_rank
    FROM monthly_plays
)

SELECT * FROM ranked
WHERE monthly_rank <= 20
ORDER BY month DESC, monthly_rank
