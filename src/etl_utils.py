import json
from datetime import datetime

def generate_pyspark_code(df_metadata, target): # Renamed df to df_metadata for clarity
    target = target.lower()
    if target == "databricks":
        spark_config = "# Running on Databricks environment\n"
    elif target == "emr":
        spark_config = "# Running on AWS EMR cluster\n"
    elif target == "glue":
        spark_config = "# Running on AWS Glue job\nimport sys\nfrom awsglue.transforms import *\nfrom awsglue.utils import getResolvedOptions\nfrom awsglue.context import GlueContext\nfrom pyspark.context import SparkContext\nsc = SparkContext()\nglueContext = GlueContext(sc)\nspark = glueContext.spark_session\n"
    else:
        spark_config = ""

    # Actual ETL logic should be dynamically generated based on df_metadata.
    # The df_metadata parameter is intended to be the specific sheet/DataFrame containing the ETL mappings.
    # Example of how df_metadata could be referenced (actual implementation depends on its structure):
    # source_info = df_metadata.iloc[0]['Source Table'] if not df_metadata.empty else "default_source_path"
    # target_info = df_metadata.iloc[0]['Target Table'] if not df_metadata.empty else "default_target_path"

    if target in ("databricks", "emr", "glue"):
        common_code = """
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("ETLJob").getOrCreate()
"""
        spark_code = spark_config + common_code + """# ETL logic (intended to be based on df_metadata)
# Metadata input sample (if DataFrame): {str(df_metadata.head()) if hasattr(df_metadata, 'head') else 'df_metadata not a DataFrame or is None'}
# Target Platform: {target}

# Dynamic ETL logic generation based on df_metadata:
"""
        if hasattr(df_metadata, 'iterrows'):  # Check if it's a DataFrame
            source_table = df_metadata.iloc[0].get('Source Table Name', 'unknown_source')
            source_path = df_metadata.iloc[0].get('Source Path', 's3://test/source')
            target_path = df_metadata.iloc[0].get('Target Path', 's3://test/target')

            spark_code += f"""df = spark.read.option("header", True).csv('{source_path}')
df_transformed = df.select("*")  # Basic transformation - replace with actual logic from metadata
df_transformed.write.mode("overwrite").parquet('{target_path}')

# Source Table: {source_table}, Source Path: {source_path}
# Target Path: {target_path}
"""
        else:
            spark_code += """# No DataFrame-like metadata provided. Using default paths.
df = spark.read.option("header", True).csv("s3://default/source/path")
df_transformed = df.select("*")
df_transformed.write.mode("overwrite").parquet("s3://default/target/path")
"""
    else:
        spark_code = "# Unsupported target platform.\n"

    return spark_code

# Lineage JSON builder for Collibra single-file spec
def generate_lineage_json(df): # df here is the dictionary of DataFrames from the Excel file
    try:
        source_df = df.get("Source Metadata")
        target_df = df.get("Target Metadata")

        if source_df is None:
            return json.dumps({"error": "Sheet 'Source Metadata' not found in the input."})
        if target_df is None:
            return json.dumps({"error": "Sheet 'Target Metadata' not found in the input."})

        lineage = {
            "codeGraph": {
                "nodes": [],
                "edges": []
            },
            "sourceProperties": {
                "platform": "Custom Platform",
                "version": "1.0",
                "description": "Generated from metadata spreadsheet"
            },
            "targetProperties": {
                "collibraPlatformVersion": "2024.1",
                "importTimestamp": datetime.now().isoformat()
            },
            "script": {
                "format": "PYTHON",
                "code": "# Transformation logic generated dynamically\n# See ETL job for full implementation"
            }
        }

        for _, row in source_df.iterrows():
            lineage["codeGraph"]["nodes"].append({
                "id": f"src.{row.get('Source Table Name', 'unknown_table')}.{row.get('Source Column Name', 'unknown_column')}",
                "type": "Column",
                "name": row.get('Source Column Name', 'unknown_column'),
                "description": row.get('Comments', '')
            })

        for _, row in target_df.iterrows():
            tgt_id = f"tgt.{row.get('Target Table Name', 'unknown_table')}.{row.get('Target Column Name', 'unknown_column')}"
            lineage["codeGraph"]["nodes"].append({
                "id": tgt_id,
                "type": "Column",
                "name": row.get('Target Column Name', 'unknown_column'),
                "description": row.get('Transformation Logic', '')
            })
            # Ensure source columns referenced in target exist or handle missing references gracefully
            source_table = row.get('Source Table Name', 'unknown_source_table')
            source_column = row.get('Source Column Name', 'unknown_source_column')
            lineage["codeGraph"]["edges"].append({
                "source": f"src.{source_table}.{source_column}",
                "target": tgt_id
            })

        return json.dumps(lineage, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})

# Data Quality test generator using Great Expectations and PyTest
def generate_data_quality_tests(df): # df here is the dictionary of DataFrames
    try:
        rules_df = df.get("Data Quality Rules")
        if rules_df is None:
            return "# Error generating tests: Sheet 'Data Quality Rules' not found in the input."

        expectations_code = """
# Auto-generated data quality tests using Great Expectations + PyTest
# Save this file as test_dq.py and run manually using: pytest test_dq.py

import great_expectations as ge
import pytest

@pytest.fixture
def load_data(data_file="sample_data.csv"):
    # Making 'sample_data.csv' configurable or passed as an argument
    return ge.read_csv(data_file)
"""

        test_block = ""
        for idx, row in rules_df.iterrows():
            validations = row.get("Validations", "")
            column_name = row.get("Column Name", f"column_{idx}") # Use a default if 'Column Name' is missing

            if "Not Null" in validations:
                test_block += f"""

def test_column_{column_name}_not_null(load_data):
    assert load_data.expect_column_values_to_not_be_null('{column_name}')['success']
"""

            if "Range" in validations:
                min_value = row.get("Min Value", 0) # Default Min Value if not specified
                max_value = row.get("Max Value", 100) # Default Max Value if not specified
                test_block += f"""

def test_column_{column_name}_in_range(load_data):
    assert load_data.expect_column_values_to_be_between('{column_name}', min_value={min_value}, max_value={max_value})['success']
"""
            # Add more validation types here as needed, e.g., Set, Regex, Uniqueness

        if not test_block:
            test_block = "\n# No specific validation rules found or parsed to generate tests.\n"

        # Add optional main block for manual runs
        test_block += """

if __name__ == "__main__":
    import pytest
    # This will run all tests in this file
    raise SystemExit(pytest.main([__file__]))
"""
        return expectations_code + test_block
    except Exception as e:
        return f"# Error generating tests: {str(e)}"
