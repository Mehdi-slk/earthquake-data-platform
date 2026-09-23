{{ config(
    materialized='incremental',
    unique_key='location_key',
    on_schema_change='append_new_columns'
) }}

WITH new_locations AS (

    SELECT DISTINCT
        {{ dbt_utils.generate_surrogate_key([
            'country',
            'region'
        ]) }} AS location_key,
        country,
        region
    FROM {{ ref('staging_table') }}

)

SELECT
    location_key,
    country,
    region

FROM new_locations

{% if is_incremental() %}

WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} AS l
    WHERE l.location_key = new_locations.location_key
)

{% endif %}