{{ config(
    materialized='incremental',
    unique_key='network_event_code',
    on_schema_change='append_new_columns'
) }}

WITH new_networks AS (

    SELECT DISTINCT
        network_event_code,
        source_network,
        id_sources
    FROM {{ ref('staging_table') }}

)

SELECT *
FROM new_networks

{% if is_incremental() %}

WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} AS n
    WHERE n.network_event_code = new_networks.network_event_code
)

{% endif %}