
-- Generic Vault tests that can be applied to any Vault model

-- Test that all hub keys are properly formatted
SELECT 'hub_key_format' AS test_name, COUNT(*) AS failures
FROM {{ ref('hub_customer') }}
WHERE hub_customer_key IS NULL 
   OR LENGTH(hub_customer_key) != 32
   OR hub_customer_key !~ '^[A-F0-9]+$'

UNION ALL

-- Test that load_datetime is never null
SELECT 'load_datetime_not_null' AS test_name, COUNT(*) AS failures
FROM {{ ref('hub_customer') }}
WHERE load_datetime IS NULL

UNION ALL

-- Test that record_source is never null or empty
SELECT 'record_source_not_null' AS test_name, COUNT(*) AS failures
FROM {{ ref('hub_customer') }}
WHERE record_source IS NULL OR TRIM(record_source) = ''
