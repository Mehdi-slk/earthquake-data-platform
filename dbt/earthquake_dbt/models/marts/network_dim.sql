SELECT DISTINCT
    network_event_code,
    source_network,
    id_sources
FROM {{ ref('staging_table') }}