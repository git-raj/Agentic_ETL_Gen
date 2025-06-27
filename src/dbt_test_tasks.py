import pandas as pd
import json

def generate_dbt_tests(metadata, llm, use_agentic=False):
    """
    Generates dbT-specific data quality tests from metadata.
    Creates schema.yml files with test definitions and custom test macros.
    """
    print("Generating dbT data quality tests...")
    
    if metadata.empty:
        return {"error": "Missing metadata for dbT test generation."}
    
    test_files = {}
    
    # Generate schema.yml files with tests
    schema_files = _generate_schema_yml_files(metadata, llm, use_agentic)
    test_files.update(schema_files)
    
    # Generate custom test macros
    custom_macros = _generate_custom_test_macros(metadata, llm, use_agentic)
    test_files.update(custom_macros)
    
    # Generate vault-specific tests
    vault_tests = _generate_vault_specific_tests(metadata, llm, use_agentic)
    test_files.update(vault_tests)
    
    return test_files

def _generate_schema_yml_files(metadata, llm, use_agentic=False):
    """Generate schema.yml files with dbT test definitions"""
    schema_files = {}
    
    # Group by table/model
    if 'Table Name' in metadata.columns:
        tables = metadata['Table Name'].unique()
        
        for table in tables:
            table_metadata = metadata[metadata['Table Name'] == table]
            
            if use_agentic and llm:
                schema_content = _generate_schema_yml_with_llm(table, table_metadata, llm)
            else:
                schema_content = _generate_schema_yml_template(table, table_metadata)
            
            schema_files[f"schema_{table.lower()}.yml"] = schema_content
    
    return schema_files

def _generate_schema_yml_template(table_name, table_metadata):
    """Generate template schema.yml with dbT tests"""
    
    models_config = []
    
    # Generate tests for each column
    for _, row in table_metadata.iterrows():
        column_name = row.get('Column Name', '')
        validation = str(row.get('Validations', '')).strip()
        data_type = row.get('Data Type', '')
        description = row.get('Description', '')
        
        if not column_name:
            continue
        
        column_tests = _generate_column_tests(column_name, validation, data_type)
        
        column_config = {
            'name': column_name,
            'description': description or f"Column {column_name}",
            'tests': column_tests
        }
        
        models_config.append(column_config)
    
    # Create schema.yml structure
    schema_yml = f"""
version: 2

models:
  - name: stg_{table_name.lower()}
    description: "Staging model for {table_name}"
    columns:
"""
    
    for col_config in models_config:
        schema_yml += f"""      - name: {col_config['name']}
        description: "{col_config['description']}"
"""
        if col_config['tests']:
            schema_yml += "        tests:\n"
            for test in col_config['tests']:
                if isinstance(test, dict):
                    test_name = list(test.keys())[0]
                    test_params = test[test_name]
                    schema_yml += f"          - {test_name}:\n"
                    for param, value in test_params.items():
                        schema_yml += f"              {param}: {value}\n"
                else:
                    schema_yml += f"          - {test}\n"
    
    return schema_yml

def _generate_column_tests(column_name, validation, data_type):
    """Generate dbT tests for a specific column"""
    tests = []
    
    if not validation or validation == 'nan':
        return tests
    
    validation_lower = validation.lower()
    
    # Not null test
    if 'not null' in validation_lower:
        tests.append('not_null')
    
    # Unique test
    if 'unique' in validation_lower or 'primary key' in validation_lower:
        tests.append('unique')
    
    # Range tests
    if 'range' in validation_lower:
        try:
            # Extract range values: range(min, max)
            range_part = validation_lower.split('range(')[1].split(')')[0]
            min_val, max_val = [x.strip() for x in range_part.split(',')]
            tests.append({
                'dbt_utils.accepted_range': {
                    'min_value': min_val,
                    'max_value': max_val
                }
            })
        except:
            tests.append('# Invalid range format in validation')
    
    # Accepted values test
    if 'in(' in validation_lower:
        try:
            # Extract accepted values: in(val1, val2, val3)
            values_part = validation_lower.split('in(')[1].split(')')[0]
            accepted_values = [x.strip().strip("'\"") for x in values_part.split(',')]
            tests.append({
                'accepted_values': {
                    'values': accepted_values
                }
            })
        except:
            tests.append('# Invalid accepted values format')
    
    # Regex pattern test
    if 'regex' in validation_lower or 'pattern' in validation_lower:
        try:
            # Extract regex pattern
            if 'regex(' in validation_lower:
                pattern = validation_lower.split('regex(')[1].split(')')[0].strip("'\"")
            else:
                pattern = validation_lower.split('pattern(')[1].split(')')[0].strip("'\"")
            
            tests.append({
                'dbt_utils.accepted_range': {
                    'regex': pattern
                }
            })
        except:
            tests.append('# Invalid regex format')
    
    # Length tests for string columns
    if 'varchar' in data_type.lower() or 'string' in data_type.lower():
        if 'length' in validation_lower:
            try:
                # Extract length constraint
                if 'max_length' in validation_lower:
                    max_len = validation_lower.split('max_length(')[1].split(')')[0]
                    tests.append({
                        'dbt_utils.expression_is_true': {
                            'expression': f'length({column_name}) <= {max_len}'
                        }
                    })
                elif 'min_length' in validation_lower:
                    min_len = validation_lower.split('min_length(')[1].split(')')[0]
                    tests.append({
                        'dbt_utils.expression_is_true': {
                            'expression': f'length({column_name}) >= {min_len}'
                        }
                    })
            except:
                tests.append('# Invalid length format')
    
    return tests

def _generate_custom_test_macros(metadata, llm, use_agentic=False):
    """Generate custom dbT test macros for Vault-specific validations"""
    macros = {}
    
    # Hash key format test macro
    hash_key_test = """
{% macro test_hash_key_format(model, column_name) %}
    SELECT *
    FROM {{ model }}
    WHERE {{ column_name }} IS NULL
       OR LENGTH({{ column_name }}) != 32
       OR {{ column_name }} !~ '^[A-F0-9]+$'
{% endmacro %}
"""
    
    # Satellite chronology test macro
    satellite_chronology_test = """
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
"""
    
    # Hub integrity test macro
    hub_integrity_test = """
{% macro test_hub_integrity(model, business_key_column) %}
    SELECT *
    FROM {{ model }}
    WHERE {{ business_key_column }} IS NULL
       OR TRIM({{ business_key_column }}) = ''
{% endmacro %}
"""
    
    # Link relationship test macro
    link_relationship_test = """
{% macro test_link_relationship(model, hub_key_columns) %}
    {% set hub_keys = hub_key_columns.split(',') %}
    SELECT *
    FROM {{ model }}
    WHERE {% for hub_key in hub_keys %}
        {{ hub_key.strip() }} IS NULL
        {% if not loop.last %} OR {% endif %}
    {% endfor %}
{% endmacro %}
"""
    
    # Record source consistency test
    record_source_test = """
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
"""
    
    macros['vault_test_macros.sql'] = f"""
-- Custom dbT test macros for Data Vault 2.0

{hash_key_test}

{satellite_chronology_test}

{hub_integrity_test}

{link_relationship_test}

{record_source_test}
"""
    
    return macros

def _generate_vault_specific_tests(metadata, llm, use_agentic=False):
    """Generate Vault-specific test files"""
    vault_tests = {}
    
    # Generate generic Vault tests
    generic_vault_tests = """
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
"""
    
    vault_tests['generic_vault_tests.sql'] = generic_vault_tests
    
    return vault_tests

def _generate_schema_yml_with_llm(table_name, table_metadata, llm):
    """Generate schema.yml using LLM"""
    columns_info = []
    for _, row in table_metadata.iterrows():
        columns_info.append({
            'name': row.get('Column Name', ''),
            'type': row.get('Data Type', ''),
            'validation': row.get('Validations', ''),
            'description': row.get('Description', '')
        })
    
    prompt = f"""
Generate a dbT schema.yml file for the following table with comprehensive data quality tests:

Table: {table_name}
Columns: {json.dumps(columns_info, indent=2)}

Requirements:
1. Include appropriate dbT built-in tests (not_null, unique, accepted_values, relationships)
2. Use dbt_utils tests where appropriate (accepted_range, expression_is_true, etc.)
3. Add custom Vault-specific tests for hash keys and chronology
4. Include proper descriptions for all models and columns
5. Follow dbT schema.yml best practices

Return only the YAML content.
"""
    
    try:
        response = llm.invoke([{"role": "user", "content": prompt}])
        return response.content.strip()
    except Exception as e:
        print(f"LLM schema.yml generation failed: {e}")
        return _generate_schema_yml_template(table_name, table_metadata)

def generate_dbt_sources_yml(source_metadata):
    """Generate sources.yml file for raw data sources"""
    
    if source_metadata.empty:
        return ""
    
    # Get unique source tables
    source_tables = source_metadata['Table Name'].unique() if 'Table Name' in source_metadata.columns else []
    
    sources_yml = """
version: 2

sources:
  - name: raw
    description: "Raw data sources"
    tables:
"""
    
    for table in source_tables:
        table_columns = source_metadata[source_metadata['Table Name'] == table]
        
        sources_yml += f"""      - name: {table.lower()}
        description: "Raw {table} data"
        columns:
"""
        
        for _, row in table_columns.iterrows():
            column_name = row.get('Column Name', '')
            description = row.get('Description', '')
            data_type = row.get('Data Type', '')
            
            sources_yml += f"""          - name: {column_name}
            description: "{description or f'{column_name} column'}"
            data_type: {data_type}
"""
    
    return sources_yml

def generate_dbt_test_suite():
    """Generate a comprehensive dbT test suite configuration"""
    
    test_suite = {
        "data_quality_tests.yml": """
# Comprehensive data quality test suite for Vault models

version: 2

models:
  # Hub model tests
  - name: hub_customer
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - hub_customer_key
            - load_datetime
      - vault_test_macros.test_hub_integrity:
          business_key_column: customer_id
      - vault_test_macros.test_hash_key_format:
          column_name: hub_customer_key

  # Link model tests  
  - name: link_customer_order
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - link_customer_order_key
            - load_datetime
      - vault_test_macros.test_link_relationship:
          hub_key_columns: "hub_customer_key, hub_order_key"

  # Satellite model tests
  - name: sat_customer_details
    tests:
      - vault_test_macros.test_satellite_chronology:
          hub_key_column: hub_customer_key
          load_datetime_column: load_datetime
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - hub_customer_key
            - load_datetime
""",
        
        "test_config.yml": """
# Test configuration for dbT project

version: 2

tests:
  # Store test failures for analysis
  +store_failures: true
  +store_failures_as: table
  
  # Test severity levels
  +severity: error  # Default severity
  
  # Test tags for organization
  +tags:
    - data_quality
    - vault_integrity

# Custom test configurations
vault_tests:
  hash_key_tests:
    +severity: error
    +tags: [hash_key, critical]
  
  chronology_tests:
    +severity: warn
    +tags: [chronology, temporal]
  
  relationship_tests:
    +severity: error
    +tags: [relationships, referential_integrity]
"""
    }
    
    return test_suite
