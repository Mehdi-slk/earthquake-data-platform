SELECT DISTINCT
    TO_CHAR(event_time, 'YYYYMMDD') AS date_key,
    event_time::date AS full_date,
    event_year AS the_year,
    event_month AS the_month,
    event_day AS the_day

FROM {{ ref('staging_table') }}