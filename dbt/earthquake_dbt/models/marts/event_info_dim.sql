{{ config(
    materialized='incremental',
    unique_key='event_id',
    on_schema_change='append_new_columns'
) }}

WITH new_events AS (

    SELECT DISTINCT
        event_id,
        event_status,
        event_type,
        event_title,
        magnitude_type,
        available_data_types,
        associated_ids
    FROM {{ ref('staging_table') }}

)

SELECT *
FROM new_events

{% if is_incremental() %}

WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} AS t
    WHERE t.event_id = new_events.event_id
)

{% endif %}