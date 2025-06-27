
-- Custom dbT test macros for Data Vault 2.0


{% macro test_hash_key_format(model, column_name) %}
    SELECT *
    FROM {{ model }}
    WHERE {{ column_name }} IS NULL
       OR LENGTH({{ column_name }}) != 32
       OR {{ column_name }} !~ '^[A-F0-9]+$'
{% endmacro %}



{% macro test_satellite_chronology(model, hub_key_column, load_datetime_column) %}
    WITH chronology_check AS (
        SELECT 
            {{ hub_key_column }},
            {{ load_datetime_column }},
            LAG({{ load_datetime_column }}) OVER (
                PARTITION BY {{ hub_key_column }} 
                ORDER BY {{ load_datetime_column }}
            ) AS prev_load_datetime
        FROM {{ model }}
    )
    SELECT *
    FROM chronology_check
    WHERE {{ load_datetime_column }} <= prev_load_datetime
{% endmacro %}



{% macro test_hub_integrity(model, business_key_column) %}
    SELECT *
    FROM {{ model }}
    WHERE {{ business_key_column }} IS NULL
       OR TRIM({{ business_key_column }}) = ''
{% endmacro %}



{% macro test_link_relationship(model, hub_key_columns) %}
    {% set hub_keys = hub_key_columns.split(',') %}
    SELECT *
    FROM {{ model }}
    WHERE {% for hub_key in hub_keys %}
        {{ hub_key.strip() }} IS NULL
        {% if not loop.last %} OR {% endif %}
    {% endfor %}
{% endmacro %}



{% macro test_record_source_consistency(model, record_source_column) %}
    SELECT *
    FROM {{ model }}
    WHERE {{ record_source_column }} IS NULL
       OR TRIM({{ record_source_column }}) = ''
       OR {{ record_source_column }} NOT IN (
           SELECT DISTINCT record_source 
           FROM {{ ref('dim_record_sources') }}
       )
{% endmacro %}

