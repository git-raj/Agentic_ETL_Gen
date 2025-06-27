
{{
    config(
        materialized='incremental',
        unique_key=['hub_key', 'load_datetime']
    )
}}

WITH source_data AS (
    SELECT
        -- Link to hub key
        hub_key,
        , , , ,
        CURRENT_TIMESTAMP AS load_datetime,
        'system' AS record_source
    FROM {{ ref('stg_') }}
),

hashed AS (
    SELECT
        hub_key,
        {{ dbt_utils.generate_surrogate_key(['', '', '', '']) }} AS hash_diff,
        , , , ,
        load_datetime,
        record_source
    FROM source_data
)

SELECT * FROM hashed

{% if is_incremental() %}
    WHERE load_datetime > (SELECT MAX(load_datetime) FROM {{ this }})
{% endif %}
