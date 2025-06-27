# dbT Raw Vault Feature Documentation

## Overview

The ETL Automation Platform now supports **dbT Raw Vault** as a target platform, enabling automatic generation of Data Vault 2.0 models using dbT (data build tool). This feature provides a modern, scalable approach to data warehousing with built-in testing, documentation, and version control.

## Features

### 🏗️ Data Vault 2.0 Model Generation
- **Hub Models**: Business key entities with hash keys and load metadata
- **Link Models**: Relationships between business entities
- **Satellite Models**: Descriptive attributes with change tracking
- **Staging Models**: Raw data preparation and cleansing

### 🧪 dbT-Native Testing Framework
- **Built-in Tests**: not_null, unique, accepted_values, relationships
- **Custom Vault Macros**: Hash key validation, chronology checks, integrity tests
- **Schema Definitions**: Comprehensive test configurations in YAML
- **Test Suites**: Organized by model type and criticality

### ⚙️ Complete dbT Project Structure
- **dbt_project.yml**: Project configuration with model hierarchies
- **profiles.yml**: Data warehouse connection profiles
- **packages.yml**: Required dbT packages (dbt_utils, audit_helper)
- **sources.yml**: Raw data source definitions

### 🛫 dbT-Specific Airflow Orchestration
- **dbT Commands**: deps, run, test, docs generate
- **Staged Execution**: staging → vault → testing → documentation
- **Error Handling**: Retry logic and failure notifications
- **Scheduling**: Configurable intervals with dependency management

## How to Use

### 1. Select Target Platform
In the Streamlit interface, choose **"dbT Raw Vault"** from the Target Platform dropdown.

### 2. Upload Metadata
Upload your Excel metadata file with the following sheets:
- **Source Metadata**: Table and column definitions
- **Target Metadata**: Validation rules and constraints
- **Mapping Metadata**: Join conditions and relationships
- **ETL Metadata**: Scheduling and ownership information
- **Data Quality Rules**: Validation and testing requirements

### 3. Configure Generation Options
- ✅ **Generate ETL Code**: Creates dbT Vault models
- ✅ **Generate DQ Tests**: Creates dbT test suites
- ✅ **Generate Lineage**: Creates data lineage documentation
- ✅ **Generate Airflow DAG**: Creates dbT orchestration

### 4. Enable Agentic Generation (Optional)
Check **"Use Agentic LLM-based Generation"** for AI-enhanced model creation.

## Generated Output Structure

```
output/dbt/
├── dbt_project.yml          # Project configuration
├── profiles.yml             # Connection profiles
├── packages.yml             # Package dependencies
├── models/
│   ├── sources.yml          # Raw data sources
│   ├── staging/             # Staging models
│   │   ├── stg_customers.sql
│   │   └── stg_orders.sql
│   └── vault/               # Vault models
│       ├── hub_customer.sql
│       ├── hub_order.sql
│       ├── link_customer_order.sql
│       ├── sat_customer_details.sql
│       └── sat_order_details.sql
├── tests/                   # Custom test files
│   ├── schema_customers.yml
│   ├── schema_orders.yml
│   └── data_quality_tests.yml
└── macros/                  # Custom test macros
    ├── vault_test_macros.sql
    └── generic_vault_tests.sql
```

## Sample Generated Models

### Hub Model Example
```sql
{{
    config(
        materialized='incremental',
        unique_key='hub_customer_key'
    )
}}

WITH source_data AS (
    SELECT DISTINCT
        customer_id,
        CURRENT_TIMESTAMP AS load_datetime,
        'system' AS record_source
    FROM {{ ref('stg_customers') }}
    WHERE customer_id IS NOT NULL
),

hashed AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} AS hub_customer_key,
        customer_id,
        load_datetime,
        record_source
    FROM source_data
)

SELECT * FROM hashed

{% if is_incremental() %}
    WHERE load_datetime > (SELECT MAX(load_datetime) FROM {{ this }})
{% endif %}
```

### Satellite Model Example
```sql
{{
    config(
        materialized='incremental',
        unique_key=['hub_key', 'load_datetime']
    )
}}

WITH source_data AS (
    SELECT
        hub_key,
        customer_name,
        customer_email,
        CURRENT_TIMESTAMP AS load_datetime,
        'system' AS record_source
    FROM {{ ref('stg_customers') }}
),

hashed AS (
    SELECT
        hub_key,
        {{ dbt_utils.generate_surrogate_key(['customer_name', 'customer_email']) }} AS hash_diff,
        customer_name,
        customer_email,
        load_datetime,
        record_source
    FROM source_data
)

SELECT * FROM hashed

{% if is_incremental() %}
    WHERE load_datetime > (SELECT MAX(load_datetime) FROM {{ this }})
{% endif %}
```

## Custom Test Macros

### Hash Key Validation
```sql
{% macro test_hash_key_format(model, column_name) %}
    SELECT *
    FROM {{ model }}
    WHERE {{ column_name }} IS NULL
       OR LENGTH({{ column_name }}) != 32
       OR {{ column_name }} !~ '^[A-F0-9]+$'
{% endmacro %}
```

### Satellite Chronology Test
```sql
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
```

## Airflow DAG Example

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dbt_vault_pipeline',
    default_args=default_args,
    description='dbT Data Vault 2.0 Pipeline',
    schedule_interval='@daily',
    catchup=False,
    tags=['dbt', 'vault', 'data_warehouse']
)

# dbT tasks
dbt_deps = BashOperator(
    task_id='dbt_deps',
    bash_command='cd /path/to/dbt/project && dbt deps',
    dag=dag
)

dbt_run_staging = BashOperator(
    task_id='dbt_run_staging',
    bash_command='cd /path/to/dbt/project && dbt run --models staging',
    dag=dag
)

dbt_run_vault = BashOperator(
    task_id='dbt_run_vault',
    bash_command='cd /path/to/dbt/project && dbt run --models vault',
    dag=dag
)

dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command='cd /path/to/dbt/project && dbt test',
    dag=dag
)

dbt_docs_generate = BashOperator(
    task_id='dbt_docs_generate',
    bash_command='cd /path/to/dbt/project && dbt docs generate',
    dag=dag
)

# Task dependencies
dbt_deps >> dbt_run_staging >> dbt_run_vault >> dbt_test >> dbt_docs_generate
```

## Benefits

### 🚀 **Modern Data Architecture**
- Data Vault 2.0 methodology for scalable data warehousing
- Separation of concerns with staging and vault layers
- Built-in historization and auditability

### 🧪 **Test-Driven Development**
- Comprehensive data quality testing
- Vault-specific validation rules
- Automated test execution in CI/CD pipelines

### 📚 **Documentation & Lineage**
- Auto-generated data documentation
- Column-level lineage tracking
- Business glossary integration

### 🔄 **Version Control & Collaboration**
- Git-based version control for all models
- Code review processes for data transformations
- Collaborative development workflows

### ⚡ **Performance & Scalability**
- Incremental model processing
- Optimized for cloud data warehouses
- Parallel execution capabilities

## Requirements

### Data Warehouse Support
- Snowflake
- BigQuery
- Redshift
- Databricks
- PostgreSQL

### dbT Dependencies
- dbt-core >= 1.0.0
- dbt_utils package
- audit_helper package

### Airflow Integration
- Apache Airflow >= 2.0.0
- BashOperator or dbT Cloud Operator
- Proper dbT CLI installation

## Demo

Run the included demo script to see the feature in action:

```bash
python demo_dbt_vault.py
```

This will generate sample dbT Vault models and demonstrate all the key features.

## Testing

Run the test suite to verify functionality:

```bash
python -m pytest tests/test_dbt_vault_tasks.py -v
```

## Next Steps

1. **Configure your data warehouse connection** in profiles.yml
2. **Customize the generated models** for your specific business logic
3. **Set up your Airflow environment** to run the generated DAGs
4. **Implement CI/CD pipelines** for automated testing and deployment
5. **Train your team** on dbT and Data Vault methodologies

## Support

For questions or issues with the dbT Raw Vault feature, please refer to:
- [dbT Documentation](https://docs.getdbt.com/)
- [Data Vault 2.0 Methodology](https://www.data-vault.co.uk/)
- [Project Issues](https://github.com/your-repo/issues)
