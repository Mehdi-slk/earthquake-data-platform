SELECT DISTINCT
    event_id,
    event_status,
    event_type,
    event_title,
    magnitude_type,
    available_data_types,
    associated_ids
FROM {{ ref('staging_table') }}