{{ config(
    materialized='incremental',
    unique_key='date_key',
    on_schema_change='append_new_columns'
) }}

WITH new_dates AS (

    SELECT DISTINCT
        TO_CHAR(event_time, 'YYYYMMDD') AS date_key,
        event_time::date AS full_date,
        event_year AS the_year,
        event_month AS the_month,
        event_day AS the_day
    FROM {{ ref('staging_table') }}

)

SELECT *
FROM new_dates

{% if is_incremental() %}

WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} AS d
    WHERE d.date_key = new_dates.date_key
)

{% endif %}