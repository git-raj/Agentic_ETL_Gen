import re

def generate_etl_code(source_metadata, target_metadata, target_platform, llm, use_agentic=False):
    """
    Generates PySpark code for ETL based on the provided metadata and target platform.
    """
    print("Generating ETL code...")

    if source_metadata.empty:
        return "Error: Source metadata is empty."
    if target_metadata.empty:
        return "Error: Target metadata is empty."

    if 'Source Table Name' not in source_metadata.columns:
        return "Error: 'Source Table Name' column not found in source metadata."
    if 'Target Table Name' not in target_metadata.columns:
        return "Error: 'Target Table Name' column not found in target metadata."

    source_table_names = source_metadata['Source Table Name'].dropna().unique()
    if len(source_table_names) == 0:
        return "Error: No non-null 'Source Table Name' found in source metadata."
    target_table_names = target_metadata['Target Table Name'].dropna().unique()
    if len(target_table_names) == 0:
        return "Error: No non-null 'Target Table Name' found in target metadata."

    source_table = source_table_names[0]
    target_table = target_table_names[0]

    transformation_lines = []
    for _, row in target_metadata.iterrows():
        target_col = row.get('Target Column Name')
        transformation = row.get('Transformation Logic') or 'source_df["<source_col>"]'

        if not target_col:
            continue

        # Extract source column name from transformation logic
        match = re.search(r"Direct mapping from (.*)", transformation)
        if match:
            source_col = match.group(1)
            transformation = f'source_df["{source_col}"]'
        else:
            transformation = 'source_df["<source_col>"]'  # Default transformation

        line = f'    target_df = target_df.withColumn("{target_col}", {transformation})'
        transformation_lines.append(line)
    print(f"Transformation lines: {transformation_lines}")

    # Platform-specific setup
    if target_platform == "Databricks":
        platform_setup = """
    # Databricks-specific setup
    spark.conf.set("spark.databricks.io.cache.enabled", "true")
"""
        write_method = f'target_df.write.format("delta").mode("overwrite").saveAsTable("{target_table}")'

    elif target_platform == "EMR":
        platform_setup = """
    # EMR-specific setup
    spark.conf.set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
"""
        write_method = f'target_df.write.mode("overwrite").saveAsTable("{target_table}")'

    elif target_platform == "AWS Glue":
        platform_setup = """
    # AWS Glue-specific setup
    import sys
    from awsglue.context import GlueContext
    from awsglue.utils import getResolvedOptions

    glueContext = GlueContext(spark.sparkContext)
"""
        write_method = f'glueContext.write_dynamic_frame.from_options(frame=DynamicFrame.fromDF(target_df, glueContext, "target"), connection_type="s3", connection_options={{"path": "s3://your-bucket/{target_table}"}}, format="parquet")'
    else:
        platform_setup = "    # Generic Spark setup"
        write_method = f'target_df.write.mode("overwrite").saveAsTable("{target_table}")'

    pyspark_code = f"""
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("ETL Job").getOrCreate()
{platform_setup}

    # Read source data
    source_df = spark.table("{source_table}")
    target_df = source_df

    # Apply transformations
{chr(10).join(transformation_lines)}

    # Write target data
    {write_method}

    spark.stop()

if __name__ == "__main__":
    main()
"""

    return pyspark_code
