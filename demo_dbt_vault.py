#!/usr/bin/env python3
"""
Demo script to showcase the new dbT Raw Vault functionality
"""

import pandas as pd
import os
from src.agents import Agents
from src.llm_utils import get_llm_instance

def create_sample_metadata():
    """Create sample metadata for demonstration"""
    
    # Source metadata with business keys and descriptive attributes
    source_metadata = pd.DataFrame({
        'Table Name': ['customers', 'customers', 'customers', 'orders', 'orders', 'orders'],
        'Column Name': ['customer_id', 'customer_name', 'customer_email', 'order_id', 'customer_id', 'order_date'],
        'Data Type': ['INTEGER', 'VARCHAR(100)', 'VARCHAR(255)', 'INTEGER', 'INTEGER', 'DATE'],
        'Description': ['Customer identifier', 'Customer full name', 'Customer email address', 'Order identifier', 'Customer reference', 'Order date']
    })
    
    # Target metadata for validation rules
    target_metadata = pd.DataFrame({
        'Table Name': ['customers', 'customers', 'orders', 'orders'],
        'Column Name': ['customer_name', 'customer_email', 'order_id', 'order_date'],
        'Data Type': ['VARCHAR(100)', 'VARCHAR(255)', 'INTEGER', 'DATE'],
        'Validations': ['not null', 'regex(^[\\w\\.-]+@[\\w\\.-]+\\.[a-zA-Z]{2,}$)', 'unique', 'not null']
    })
    
    # Mapping metadata for relationships
    mapping_metadata = pd.DataFrame({
        'Join Conditions': ['customers.customer_id = orders.customer_id'],
        'Business Rule / Expression': ['Customer to Order relationship'],
        'Filter Conditions': ['orders.order_date >= \'2023-01-01\''],
        'Lookup Table Used': [''],
        'Aggregation Rule': ['']
    })
    
    # ETL metadata for scheduling
    etl_metadata = pd.DataFrame({
        'Load Frequency': ['@daily'],
        'ETL Job Owner': ['data_engineering'],
        'Dependencies': ['raw_data_ingestion'],
        'Error Handling Logic': ['log and continue'],
        'Pre/Post Load Script': ['validate_source_data.py;generate_reports.py']
    })
    
    # Data Quality rules
    dq_rules = pd.DataFrame({
        'Validations': ['not null', 'unique', 'range(0, 1000000)', 'regex(^[A-Za-z\\s]+$)'],
        'Reject Handling': ['quarantine', 'fail_job', 'log_warning', 'quarantine'],
        'Record Count Expected': [1000, 1000, 5000, 1000]
    })
    
    return source_metadata, target_metadata, mapping_metadata, etl_metadata, dq_rules

def demo_dbt_vault_generation():
    """Demonstrate dbT Raw Vault code generation"""
    
    print("🚀 dbT Raw Vault Generation Demo")
    print("=" * 50)
    
    # Create sample metadata
    source_metadata, target_metadata, mapping_metadata, etl_metadata, dq_rules = create_sample_metadata()
    
    print("📊 Sample Metadata Created:")
    print(f"  - Source tables: {source_metadata['Table Name'].unique()}")
    print(f"  - Target validations: {len(target_metadata)} rules")
    print(f"  - Mapping relationships: {len(mapping_metadata)} joins")
    print()
    
    # Initialize agents (without LLM for demo)
    print("🤖 Initializing Agents...")
    agents = Agents(llm=None)
    
    # Generate dbT Vault models
    print("🏗️ Generating dbT Vault 2.0 Models...")
    results = agents.run(
        source_metadata_df=source_metadata,
        target_metadata_df=target_metadata,
        mapping_metadata_df=mapping_metadata,
        etl_metadata_df=etl_metadata,
        dq_rules_df=dq_rules,
        target_platform="dbT Raw Vault",
        use_agentic=False,
        generate_etl=True,
        generate_dq=True,
        generate_lineage=True,
        generate_airflow=True
    )
    
    print("✅ Generation Complete!")
    print()
    
    # Display results
    if "etl" in results:
        etl_result = results["etl"]
        
        if "vault_models" in etl_result:
            vault_models = etl_result["vault_models"]
            print(f"📁 Generated {len(vault_models)} dbT Vault Models:")
            
            for model_name in vault_models.keys():
                model_type = "🏢 Hub" if model_name.startswith("hub_") else \
                           "🔗 Link" if model_name.startswith("link_") else \
                           "🛰️ Satellite" if model_name.startswith("sat_") else \
                           "📋 Staging"
                print(f"  {model_type}: {model_name}")
            print()
        
        if "project_files" in etl_result:
            project_files = etl_result["project_files"]
            print(f"⚙️ Generated {len(project_files)} dbT Project Files:")
            for file_name in project_files.keys():
                print(f"  📄 {file_name}")
            print()
    
    if "dq" in results:
        dq_result = results["dq"]
        if isinstance(dq_result, dict):
            print(f"🧪 Generated {len(dq_result)} dbT Test Files:")
            for test_name in dq_result.keys():
                test_type = "🔧 Macro" if test_name.endswith(".sql") else "📋 Schema"
                print(f"  {test_type}: {test_name}")
            print()
    
    if "airflow_dag" in results:
        print("🛫 Generated dbT-specific Airflow DAG")
        print("  - Includes dbT deps, run, test, and docs tasks")
        print("  - Configured for Data Vault 2.0 workflow")
        print()
    
    # Show sample model content
    if "etl" in results and "vault_models" in results["etl"]:
        vault_models = results["etl"]["vault_models"]
        
        # Show a sample hub model
        hub_models = [name for name in vault_models.keys() if name.startswith("hub_")]
        if hub_models:
            sample_hub = hub_models[0]
            print(f"📖 Sample Hub Model ({sample_hub}):")
            print("-" * 40)
            print(vault_models[sample_hub][:300] + "...")
            print()
        
        # Show a sample staging model
        staging_models = [name for name in vault_models.keys() if name.startswith("stg_")]
        if staging_models:
            sample_staging = staging_models[0]
            print(f"📖 Sample Staging Model ({sample_staging}):")
            print("-" * 40)
            print(vault_models[sample_staging][:300] + "...")
            print()
    
    print("🎉 Demo Complete!")
    print()
    print("💡 Key Features Demonstrated:")
    print("  ✅ Data Vault 2.0 compliant Hub, Link, and Satellite models")
    print("  ✅ dbT-native testing framework with custom macros")
    print("  ✅ Proper dbT project structure and configuration")
    print("  ✅ dbT-specific Airflow orchestration")
    print("  ✅ Staging models for raw data preparation")
    print("  ✅ Sources configuration for data lineage")
    print()
    print("🚀 Ready to use with your data warehouse!")

if __name__ == "__main__":
    demo_dbt_vault_generation()
