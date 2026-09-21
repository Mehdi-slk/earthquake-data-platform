SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key([
        'longitude',
        'latitude',
        'country',
        'region'
    ]) }} AS location_key,

    longitude,
    latitude,
    country,
    region

FROM {{ ref('staging_table') }}