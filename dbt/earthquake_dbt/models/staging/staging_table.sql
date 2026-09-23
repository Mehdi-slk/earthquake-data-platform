{{ config(
    materialized='incremental',
    unique_key='event_id',
    on_schema_change='append_new_columns'
) }}

SELECT
    event_id,
    magnitude,
    place,
    event_time,
    updated_time,
    felt AS felt_reports,
    cdi AS community_intensity,
    mmi AS mercalli_intensity,
    status AS event_status,
    tsunami AS tsunami_flag,
    significance AS significance_score,
    network AS source_network,
    code AS network_event_code,
    ids AS associated_ids,
    sources AS id_sources,
    types AS available_data_types,
    nst AS station_count,
    dmin AS nearest_station_distance,
    rms AS rms_residual,
    gap AS azimuthal_gap,
    magnitude_type,
    event_type,
    title AS event_title,
    longitude,
    latitude,
    depth,
    country,
    region,
    year AS event_year,
    month AS event_month,
    day AS event_day,
    hour AS event_hour

FROM {{ source('earthquake', 'earthquakes') }}

{% if is_incremental() %}

WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} AS t
    WHERE t.event_id = event_id
)

{% endif %}