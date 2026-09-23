{{ config(
    materialized='incremental',
    unique_key='event_id',
    on_schema_change='append_new_columns'
) }}

SELECT
    s.event_id,
    d.date_key,
    n.network_event_code,
    l.location_key,

    s.event_time,
    s.updated_time,

    s.magnitude,
    s.felt_reports,
    s.community_intensity,
    s.mercalli_intensity,
    s.tsunami_flag,
    s.significance_score,
    s.station_count,
    s.nearest_station_distance,
    s.rms_residual,
    s.azimuthal_gap,
    s.depth,

    s.longitude,
    s.latitude

FROM {{ ref('staging_table') }} AS s

LEFT JOIN {{ ref('date_dim') }} AS d
    ON TO_CHAR(s.event_time, 'YYYYMMDD') = d.date_key

LEFT JOIN {{ ref('network_dim') }} AS n
    ON s.network_event_code = n.network_event_code

LEFT JOIN {{ ref('location_dim') }} AS l
    ON s.country = l.country
    AND s.region = l.region

{% if is_incremental() %}

WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} AS t
    WHERE t.event_id = s.event_id
)

{% endif %}