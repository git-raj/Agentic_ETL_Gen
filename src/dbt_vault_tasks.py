import os
import json
import pandas as pd
from typing import Dict, List, Tuple

def generate_dbt_vault_models(source_metadata, target_metadata, mapping_metadata, llm, use_agentic=False):
    """
    Generates dbT Vault 2.0 models (hubs, links, satellites) based on metadata.
    """
    print("Generating dbT Vault 2.0 models...")
    
    if source_metadata.empty or target_metadata.empty:
        return {"error": "Missing source or target metadata for dbT Vault generation."}
    
    vault_models = {}
    
    # Generate Hub models
    hubs = _generate_hub_models(source_metadata, target_metadata, llm, use_agentic)
    vault_models.update(hubs)
    
    # Generate Link models
    if mapping_metadata is not None and not mapping_metadata.empty:
        links = _generate_link_models(mapping_metadata, source_metadata, llm, use_agentic)
        vault_models.update(links)
    
    # Generate Satellite models
    satellites = _generate_satellite_models(source_metadata, target_metadata, llm, use_agentic)
    vault_models.update(satellites)
    
    # Generate staging models
    staging_models = _generate_staging_models(source_metadata, llm, use_agentic)
    vault_models.update(staging_models)
    
    return vault_models

def _generate_hub_models(source_metadata, target_metadata, llm, use_agentic=False):
    """Generate Hub models for Data Vault 2.0"""
    hubs = {}
    
    # Identify business keys from metadata
    business_keys = _identify_business_keys(source_metadata, target_metadata)
    
    for hub_name, key_info in business_keys.items():
        if use_agentic and llm:
            hub_sql = _generate_hub_with_llm(hub_name, key_info, llm)
        else:
            hub_sql = _generate_hub_template(hub_name, key_info)
        
        hubs[f"hub_{hub_name.lower()}"] = hub_sql
    
    return hubs

def _generate_link_models(mapping_metadata, source_metadata, llm, use_agentic=False):
    """Generate Link models for Data Vault 2.0"""
    links = {}
    
    # Identify relationships from mapping metadata
    relationships = _identify_relationships(mapping_metadata)
    
    for link_name, rel_info in relationships.items():
        if use_agentic and llm:
            link_sql = _generate_link_with_llm(link_name, rel_info, llm)
        else:
            link_sql = _generate_link_template(link_name, rel_info)
        
        links[f"link_{link_name.lower()}"] = link_sql
    
    return links

def _generate_satellite_models(source_metadata, target_metadata, llm, use_agentic=False):
    """Generate Satellite models for Data Vault 2.0"""
    satellites = {}
    
    # Group descriptive attributes by hub
    satellite_groups = _group_attributes_for_satellites(source_metadata, target_metadata)
    
    for sat_name, attr_info in satellite_groups.items():
        if use_agentic and llm:
            sat_sql = _generate_satellite_with_llm(sat_name, attr_info, llm)
        else:
            sat_sql = _generate_satellite_template(sat_name, attr_info)
        
        satellites[f"sat_{sat_name.lower()}"] = sat_sql
    
    return satellites

def _generate_staging_models(source_metadata, llm, use_agentic=False):
    """Generate staging models for raw data preparation"""
    staging_models = {}
    
    # Get unique source tables
    source_tables = source_metadata['Table Name'].unique() if 'Table Name' in source_metadata.columns else []
    
    for table in source_tables:
        table_columns = source_metadata[source_metadata['Table Name'] == table]
        
        if use_agentic and llm:
            staging_sql = _generate_staging_with_llm(table, table_columns, llm)
        else:
            staging_sql = _generate_staging_template(table, table_columns)
        
        staging_models[f"stg_{table.lower()}"] = staging_sql
    
    return staging_models

def _identify_business_keys(source_metadata, target_metadata):
    """Identify business keys for hub creation"""
    business_keys = {}
    
    # Look for primary keys or unique identifiers
    for _, row in source_metadata.iterrows():
        column_name = row.get('Column Name', '')
        table_name = row.get('Table Name', '')
        data_type = row.get('Data Type', '')
        
        # Simple heuristic: columns with 'id', 'key', or marked as primary key
        if any(keyword in column_name.lower() for keyword in ['id', 'key', 'code']) and 'date' not in column_name.lower():
            hub_name = f"{table_name}_{column_name}".replace('_id', '').replace('_key', '')
            business_keys[hub_name] = {
                'key_column': column_name,
                'source_table': table_name,
                'data_type': data_type
            }
    
    return business_keys

def _identify_relationships(mapping_metadata):
    """Identify relationships for link creation"""
    relationships = {}
    
    if 'Join Conditions' in mapping_metadata.columns:
        for _, row in mapping_metadata.iterrows():
            join_condition = str(row.get('Join Conditions', '')).strip()
            if join_condition and join_condition != 'nan':
                # Parse join conditions to identify relationships
                link_name = f"link_{len(relationships) + 1}"
                relationships[link_name] = {
                    'join_condition': join_condition,
                    'tables': _parse_join_tables(join_condition)
                }
    
    return relationships

def _group_attributes_for_satellites(source_metadata, target_metadata):
    """Group descriptive attributes for satellite creation"""
    satellite_groups = {}
    
    # Group non-key columns by table
    for _, row in source_metadata.iterrows():
        column_name = row.get('Column Name', '')
        table_name = row.get('Table Name', '')
        data_type = row.get('Data Type', '')
        
        # Skip business key columns
        if not any(keyword in column_name.lower() for keyword in ['id', 'key']) or 'date' in column_name.lower():
            sat_name = f"{table_name}_details"
            if sat_name not in satellite_groups:
                satellite_groups[sat_name] = {'attributes': [], 'source_table': table_name}
            
            satellite_groups[sat_name]['attributes'].append({
                'column_name': column_name,
                'data_type': data_type
            })
    
    return satellite_groups

def _generate_hub_template(hub_name, key_info):
    """Generate template Hub model SQL"""
    return f"""
{{{{
    config(
        materialized='incremental',
        unique_key='hub_{hub_name.lower()}_key'
    )
}}}}

WITH source_data AS (
    SELECT DISTINCT
        {key_info['key_column']},
        CURRENT_TIMESTAMP AS load_datetime,
        'system' AS record_source
    FROM {{{{ ref('stg_{key_info['source_table'].lower()}') }}}}
    WHERE {key_info['key_column']} IS NOT NULL
),

hashed AS (
    SELECT
        {{{{ dbt_utils.generate_surrogate_key(['{key_info['key_column']}']) }}}} AS hub_{hub_name.lower()}_key,
        {key_info['key_column']},
        load_datetime,
        record_source
    FROM source_data
)

SELECT * FROM hashed

{{% if is_incremental() %}}
    WHERE load_datetime > (SELECT MAX(load_datetime) FROM {{{{ this }}}})
{{% endif %}}
"""

def _generate_link_template(link_name, rel_info):
    """Generate template Link model SQL"""
    return f"""
{{{{
    config(
        materialized='incremental',
        unique_key='link_{link_name.lower()}_key'
    )
}}}}

WITH source_data AS (
    SELECT
        -- Add your hub keys here based on join conditions
        -- {rel_info['join_condition']}
        CURRENT_TIMESTAMP AS load_datetime,
        'system' AS record_source
    FROM {{{{ ref('stg_source_table') }}}}
    -- Add appropriate joins based on relationship
),

hashed AS (
    SELECT
        {{{{ dbt_utils.generate_surrogate_key(['hub_key_1', 'hub_key_2']) }}}} AS link_{link_name.lower()}_key,
        -- hub_key_1,
        -- hub_key_2,
        load_datetime,
        record_source
    FROM source_data
)

SELECT * FROM hashed

{{% if is_incremental() %}}
    WHERE load_datetime > (SELECT MAX(load_datetime) FROM {{{{ this }}}})
{{% endif %}}
"""

def _generate_satellite_template(sat_name, attr_info):
    """Generate template Satellite model SQL"""
    attributes = ', '.join([attr['column_name'] for attr in attr_info['attributes']])
    hash_columns = [attr['column_name'] for attr in attr_info['attributes']]
    
    return f"""
{{{{
    config(
        materialized='incremental',
        unique_key=['hub_key', 'load_datetime']
    )
}}}}

WITH source_data AS (
    SELECT
        -- Link to hub key
        hub_key,
        {attributes},
        CURRENT_TIMESTAMP AS load_datetime,
        'system' AS record_source
    FROM {{{{ ref('stg_{attr_info['source_table'].lower()}') }}}}
),

hashed AS (
    SELECT
        hub_key,
        {{{{ dbt_utils.generate_surrogate_key({hash_columns}) }}}} AS hash_diff,
        {attributes},
        load_datetime,
        record_source
    FROM source_data
)

SELECT * FROM hashed

{{% if is_incremental() %}}
    WHERE load_datetime > (SELECT MAX(load_datetime) FROM {{{{ this }}}})
{{% endif %}}
"""

def _generate_staging_template(table_name, table_columns):
    """Generate template staging model SQL"""
    columns = []
    for _, row in table_columns.iterrows():
        col_name = row.get('Column Name', '')
        data_type = row.get('Data Type', '')
        columns.append(f"    {col_name}")
    
    columns_sql = ',\n'.join(columns)
    
    return f"""
{{{{
    config(
        materialized='view'
    )
}}}}

WITH source AS (
    SELECT
{columns_sql}
    FROM {{{{ source('raw', '{table_name.lower()}') }}}}
),

renamed AS (
    SELECT
        -- Add any column renaming or casting here
{columns_sql}
    FROM source
)

SELECT * FROM renamed
"""

def _parse_join_tables(join_condition):
    """Parse join condition to extract table names"""
    # Simple parser - can be enhanced
    tables = []
    words = join_condition.split()
    for word in words:
        if '.' in word:
            table = word.split('.')[0]
            if table not in tables:
                tables.append(table)
    return tables

def _generate_hub_with_llm(hub_name, key_info, llm):
    """Generate Hub model using LLM"""
    prompt = f"""
Generate a dbT Hub model SQL for Data Vault 2.0 with the following specifications:
- Hub name: {hub_name}
- Business key: {key_info['key_column']}
- Source table: {key_info['source_table']}
- Data type: {key_info['data_type']}

Requirements:
1. Use incremental materialization
2. Generate hash key using dbt_utils.generate_surrogate_key
3. Include load_datetime and record_source
4. Handle incremental logic properly
5. Follow Data Vault 2.0 standards

Return only the SQL model code.
"""
    try:
        response = llm.invoke([{"role": "user", "content": prompt}])
        return response.content.strip()
    except Exception as e:
        print(f"LLM Hub generation failed: {e}")
        return _generate_hub_template(hub_name, key_info)

def _generate_link_with_llm(link_name, rel_info, llm):
    """Generate Link model using LLM"""
    prompt = f"""
Generate a dbT Link model SQL for Data Vault 2.0 with the following specifications:
- Link name: {link_name}
- Join condition: {rel_info['join_condition']}
- Related tables: {rel_info['tables']}

Requirements:
1. Use incremental materialization
2. Generate hash key for the link
3. Reference appropriate hub keys
4. Include load_datetime and record_source
5. Follow Data Vault 2.0 standards

Return only the SQL model code.
"""
    try:
        response = llm.invoke([{"role": "user", "content": prompt}])
        return response.content.strip()
    except Exception as e:
        print(f"LLM Link generation failed: {e}")
        return _generate_link_template(link_name, rel_info)

def _generate_satellite_with_llm(sat_name, attr_info, llm):
    """Generate Satellite model using LLM"""
    attributes = [attr['column_name'] for attr in attr_info['attributes']]
    prompt = f"""
Generate a dbT Satellite model SQL for Data Vault 2.0 with the following specifications:
- Satellite name: {sat_name}
- Source table: {attr_info['source_table']}
- Attributes: {attributes}

Requirements:
1. Use incremental materialization
2. Generate hash_diff for change detection
3. Link to appropriate hub key
4. Include load_datetime and record_source
5. Handle incremental logic with hash_diff comparison
6. Follow Data Vault 2.0 standards

Return only the SQL model code.
"""
    try:
        response = llm.invoke([{"role": "user", "content": prompt}])
        return response.content.strip()
    except Exception as e:
        print(f"LLM Satellite generation failed: {e}")
        return _generate_satellite_template(sat_name, attr_info)

def _generate_staging_with_llm(table_name, table_columns, llm):
    """Generate staging model using LLM"""
    columns_info = []
    for _, row in table_columns.iterrows():
        columns_info.append({
            'name': row.get('Column Name', ''),
            'type': row.get('Data Type', ''),
            'description': row.get('Description', '')
        })
    
    prompt = f"""
Generate a dbT staging model SQL for the following table:
- Table name: {table_name}
- Columns: {columns_info}

Requirements:
1. Use view materialization
2. Reference raw source table
3. Add appropriate column casting and renaming
4. Include basic data cleansing if needed
5. Follow dbT staging model best practices

Return only the SQL model code.
"""
    try:
        response = llm.invoke([{"role": "user", "content": prompt}])
        return response.content.strip()
    except Exception as e:
        print(f"LLM Staging generation failed: {e}")
        return _generate_staging_template(table_name, table_columns)

def generate_dbt_project_files(project_name="vault_project"):
    """Generate dbT project configuration files"""
    
    dbt_project_yml = f"""
name: '{project_name}'
version: '1.0.0'
config-version: 2

# This setting configures which "profile" dbt uses for this project.
profile: '{project_name}'

# These configurations specify where dbt should look for different types of files.
model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

target-path: "target"
clean-targets:
  - "target"
  - "dbt_packages"

# Model configurations
models:
  {project_name}:
    # Staging models
    staging:
      +materialized: view
      +schema: staging
    
    # Vault models
    vault:
      +materialized: incremental
      +schema: vault
      hubs:
        +tags: ["hub"]
      links:
        +tags: ["link"]
      satellites:
        +tags: ["satellite"]

# Test configurations
tests:
  +store_failures: true
  +schema: test_failures

# Snapshot configurations
snapshots:
  {project_name}:
    +target_schema: snapshots
"""

    profiles_yml = f"""
{project_name}:
  target: dev
  outputs:
    dev:
      type: snowflake  # Change to your warehouse type
      account: your_account
      user: your_user
      password: your_password
      role: your_role
      database: your_database
      warehouse: your_warehouse
      schema: your_schema
      threads: 4
      keepalives_idle: 0
      search_path: your_search_path
"""

    packages_yml = """
packages:
  - package: dbt-labs/dbt_utils
    version: 1.1.1
  - package: dbt-labs/audit_helper
    version: 0.9.0
"""

    return {
        "dbt_project.yml": dbt_project_yml,
        "profiles.yml": profiles_yml,
        "packages.yml": packages_yml
    }
