import re

#TODO need LLM generate cde based on source, target and mapping.
def generate_etl_code(source_metadata, target_metadata, target_platform, llm, use_agentic=False, mapping_metadata=None, dq_metadata=None):
    """
    Generates complete PySpark code for ETL based on metadata.
    Includes transformation rules, data quality filters, and error segregation.
    """
    if source_metadata.empty or target_metadata.empty:
        return "Error: Missing source or target metadata."

    source_table = source_metadata['Source Table Name'].dropna().unique()[0]
    target_table = target_metadata['Target Table Name'].dropna().unique()[0]
    target_table_rejected = f"{target_table}_rejected"

    # Build transformation logic from Mapping Metadata
    transformation_lines = []
    filter_conditions = []
    join_conditions = []
    if mapping_metadata is not None:
        for _, row in mapping_metadata.iterrows():
            expr = str(row.get('Business Rule / Expression', '')).strip()
            if '=' in expr:
                target_col, logic = [x.strip() for x in expr.split('=', 1)]
                transformation_lines.append(f'    df = df.withColumn("{target_col}", {logic})')
            if row.get("Filter Conditions"):
                filter_conditions.append(row["Filter Conditions"])
            if row.get("Join Conditions"):
                join_conditions.append(row["Join Conditions"])

    dq_checks = []
    quarantine_conditions = []
    if dq_metadata is not None:
        for _, rule in dq_metadata.iterrows():
            validation = str(rule.get("Validations", "")).strip().lower()
            action = str(rule.get("Reject Handling", "")).strip().lower()
            col = rule.get("Column Name", "").strip()

            if not col:
                continue

            if validation == "not null":
                condition = f"df['{col}'].isNotNull()"
            elif validation.startswith("range"):
                try:
                    bounds = validation.split("(")[1].split(")")[0].split(',')
                    min_val, max_val = bounds[0].strip(), bounds[1].strip()
                    condition = f"(df['{col}'] >= {min_val}) & (df['{col}'] <= {max_val})"
                except Exception:
                    continue
            else:
                continue

            dq_checks.append(condition)
            if action == "quarantine":
                quarantine_conditions.append(condition)

    dq_expression = " & ".join(quarantine_conditions) if quarantine_conditions else "True"

    # Platform setup
    if target_platform == "Databricks":
        platform_setup = """
    # Databricks-specific setup
    spark.conf.set("spark.databricks.io.cache.enabled", "true")
"""
        write_valid = f'df_valid.write.format("delta").mode("overwrite").saveAsTable("{target_table}")'
        write_invalid = f'df_invalid.write.format("delta").mode("overwrite").saveAsTable("{target_table_rejected}")'

    elif target_platform == "AWS Glue":
        platform_setup = """
    # AWS Glue-specific setup
    from awsglue.context import GlueContext
    from awsglue.utils import getResolvedOptions
    glueContext = GlueContext(spark.sparkContext)
"""
        write_valid = f'glueContext.write_dynamic_frame.from_options(frame=DynamicFrame.fromDF(df_valid, glueContext, "target"), connection_type="s3", connection_options={{"path": "s3://your-bucket/{target_table}"}}, format="parquet")'
        write_invalid = f'glueContext.write_dynamic_frame.from_options(frame=DynamicFrame.fromDF(df_invalid, glueContext, "rejected"), connection_type="s3", connection_options={{"path": "s3://your-bucket/{target_table_rejected}"}}, format="parquet")'

    else:
        platform_setup = "    # Generic Spark setup"
        write_valid = f'df_valid.write.mode("overwrite").saveAsTable("{target_table}")'
        write_invalid = f'df_invalid.write.mode("overwrite").saveAsTable("{target_table_rejected}")'

    pyspark_code = f"""
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("ETL Job").getOrCreate()
{platform_setup}

    df = spark.table("{source_table}")

    # Transformations
{chr(10).join(transformation_lines)}

    # Data Quality Checks & Quarantine
    df_valid = df.filter({dq_expression})
    df_invalid = df.subtract(df_valid)

    # Write outputs
    {write_valid}
    {write_invalid}

    spark.stop()

if __name__ == "__main__":
    main()
"""

    return pyspark_code
