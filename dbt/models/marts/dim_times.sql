WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2024-01-01' as date)",
        end_date="cast('2025-12-31' as date)"
    )}}
),

enriched AS (
    SELECT
        date_day,
        EXTRACT(YEAR FROM date_day) as year,
        EXTRACT(QUARTER FROM date_day) as quarter,
        EXTRACT(MONTH FROM date_day) as month,
        EXTRACT(WEEK FROM date_day) as week_of_year,
        EXTRACT(DAY FROM date_day) as day_of_month,
        EXTRACT(DOW FROM date_day) as day_of_week,
        TO_CHAR(date_day, 'Month') as month_name,
        TO_CHAR(date_day, 'Day') as day_name,
        CASE 
            WHEN EXTRACT(DOW FROM date_day) IN (0, 6) THEN TRUE 
            ELSE FALSE 
        END as is_weekend,
        CASE 
            WHEN EXTRACT(MONTH FROM date_day) IN (12, 1, 2) THEN 'Winter'
            WHEN EXTRACT(MONTH FROM date_day) IN (3, 4, 5) THEN 'Spring'
            WHEN EXTRACT(MONTH FROM date_day) IN (6, 7, 8) THEN 'Summer'
            ELSE 'Fall'
        END as season
    FROM date_spine
)

SELECT 
    date_day::date as date_key,
    year,
    quarter,
    month,
    week_of_year,
    day_of_month,
    day_of_week,
    TRIM(month_name) as month_name,
    TRIM(day_name) as day_name,
    is_weekend,
    season
FROM enriched
