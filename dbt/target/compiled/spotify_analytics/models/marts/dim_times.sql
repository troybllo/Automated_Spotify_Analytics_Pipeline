WITH date_spine AS (
    





with rawdata as (

    

    

    with p as (
        select 0 as generated_number union all select 1
    ), unioned as (

    select

    
    p0.generated_number * power(2, 0)
     + 
    
    p1.generated_number * power(2, 1)
     + 
    
    p2.generated_number * power(2, 2)
     + 
    
    p3.generated_number * power(2, 3)
     + 
    
    p4.generated_number * power(2, 4)
     + 
    
    p5.generated_number * power(2, 5)
     + 
    
    p6.generated_number * power(2, 6)
     + 
    
    p7.generated_number * power(2, 7)
     + 
    
    p8.generated_number * power(2, 8)
     + 
    
    p9.generated_number * power(2, 9)
    
    
    + 1
    as generated_number

    from

    
    p as p0
     cross join 
    
    p as p1
     cross join 
    
    p as p2
     cross join 
    
    p as p3
     cross join 
    
    p as p4
     cross join 
    
    p as p5
     cross join 
    
    p as p6
     cross join 
    
    p as p7
     cross join 
    
    p as p8
     cross join 
    
    p as p9
    
    

    )

    select *
    from unioned
    where generated_number <= 730
    order by generated_number



),

all_periods as (

    select (
        

    cast('2024-01-01' as date) + ((interval '1 day') * (row_number() over (order by 1) - 1))


    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= cast('2025-12-31' as date)

)

select * from filtered


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