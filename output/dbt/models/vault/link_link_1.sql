
{{
    config(
        materialized='incremental',
        unique_key='link_link_1_key'
    )
}}

WITH source_data AS (
    SELECT
        -- Add your hub keys here based on join conditions
        -- o_custkey = c_custkey
        CURRENT_TIMESTAMP AS load_datetime,
        'system' AS record_source
    FROM {{ ref('stg_source_table') }}
    -- Add appropriate joins based on relationship
),

hashed AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['hub_key_1', 'hub_key_2']) }} AS link_link_1_key,
        -- hub_key_1,
        -- hub_key_2,
        load_datetime,
        record_source
    FROM source_data
)

SELECT * FROM hashed

{% if is_incremental() %}
    WHERE load_datetime > (SELECT MAX(load_datetime) FROM {{ this }})
{% endif %}
