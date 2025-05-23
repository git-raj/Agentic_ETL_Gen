```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, count, when
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
import sys

def transform_data(customer_df, order_df):
    """
    Transforms customer and order dataframes based on provided metadata.

    Args:
        customer_df (pyspark.sql.DataFrame): The customer dataframe.
        order_df (pyspark.sql.DataFrame): The order dataframe.

    Returns:
        dict: A dictionary containing the transformed dataframes, keyed by logical table name.
    """

    # Create a dictionary to store the transformed dataframes
    transformed_data = {}

    # ------------------- Customer Transformation -------------------
    # Rename columns based on target metadata for customer table
    customer_transformed_df = customer_df.select(
        col("c_custkey").alias("customer_id"),
        col("c_name").alias("customer_name")
    )

    # Store the transformed customer dataframe
    transformed_data["customer"] = customer_transformed_df

    # ------------------- Orders Transformation -------------------
    # Apply filter conditions based on the mapping instructions
    order_filtered_df = order_df.filter(col("o_orderdate") > lit("2020-01-01"))

    # Perform join with customer table based on join conditions
    order_joined_df = order_filtered_df.join(
        customer_df, order_filtered_df["o_custkey"] == customer_df["c_custkey"], "inner"
    )

    # Select and rename columns based on target metadata for orders table
    order_transformed_df = order_joined_df.select(
        col("o_orderkey").alias("order_id")
    )

    # Store the transformed order dataframe
    transformed_data["orders"] = order_transformed_df

    return transformed_data


def data_quality_checks(spark, df, table_name):
    """
    Performs data quality checks on a Spark DataFrame.

    Args:
        spark (SparkSession): The SparkSession object.
        df (DataFrame): The input DataFrame.
        table_name (str): The name of the table being checked (for logging/metadata).

    Returns:
        tuple: A tuple containing:
            - cleaned_df (DataFrame): The DataFrame after applying data quality rules.
            - quarantine_df (DataFrame): The DataFrame containing records that failed the checks.
            - record_count_valid (bool): A boolean indicating if the record count meets the expected threshold.
    """

    quarantine_df = spark.createDataFrame([], df.schema) # Initialize empty quarantine DataFrame with the same schema as df
    cleaned_df = df
    record_count_valid = False

    # --------------------------------------------------
    # Check 1: Not Null Validation
    # --------------------------------------------------
    # Identify columns with null values
    null_counts = df.select([count(lit(1)).alias("total_count")] + [count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).collect()[0].asDict()
    null_cols = [col_name for col_name, null_count in null_counts.items() if col_name != "total_count" and null_count > 0]

    if null_cols:
        # Quarantine records with null values in specified columns
        null_condition = ' OR '.join([f'({col_name} IS NULL)' for col_name in null_cols])
        quarantine_df = quarantine_df.unionByName(df.where(null_condition), allowMissingColumns=True) # Append to quarantine_df
        cleaned_df = df.where(f'NOT ({null_condition})') # Remove from cleaned_df
        print(f"Quarantined {quarantine_df.count()} records due to null values in columns: {null_cols}")
    else:
        print("No null values found in specified columns.")
    
    # --------------------------------------------------
    # Check 2: Record Count Validation
    # --------------------------------------------------
    expected_min_record_count = 0  # Expected minimum record count

    record_count = cleaned_df.count()
    record_count_valid = record_count > expected_min_record_count

    if record_count_valid:
        print(f"Record count validation passed.  Count: {record_count}, Expected minimum: {expected_min_record_count}")
    else:
        print(f"Record count validation failed. Count: {record_count}, Expected minimum: {expected_min_record_count}")

    return cleaned_df, quarantine_df, record_count_valid


def main():
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_SOURCE_PATH_CUSTOMER', 'S3_SOURCE_PATH_ORDERS', 'TARGET_DB', 'QUARANTINE_TABLE'])

    glueContext = GlueContext(SparkSession.builder.getOrCreate().sparkContext)
    spark = glueContext.spark_session

    job = glueContext.start_job(args['JOB_NAME'])

    customer_df = glueContext.create_dynamic_frame.from_options(
        format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
        connection_type="s3",
        format="csv",
        connection_options={"paths": [args['S3_SOURCE_PATH_CUSTOMER']]},
        transformation_ctx="customer_df"
    ).toDF()


    order_df = glueContext.create_dynamic_frame.from_options(
        format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
        connection_type="s3",
        format="csv",
        connection_options={"paths": [args['S3_SOURCE_PATH_ORDERS']]},
        transformation_ctx="order_df"
    ).toDF()


    transformed_data = transform_data(customer_df, order_df)

    for table_name, df in transformed_data.items():
        cleaned_df, quarantine_df, record_count_valid = data_quality_checks(spark, df, table_name)

        if cleaned_df is not None and record_count_valid:
            glueContext.write_dynamic_frame.from_options(
                frame = glueContext.create_dynamic_frame.fromDF(cleaned_df, glueContext, "cleaned_df"),
                connection_type = "s3",
                connection_options = {"path": f"s3://your-bucket/{args['TARGET_DB']}/target_{table_name}/", "partitionKeys": []}, # Replace with your bucket and path
                format = "parquet",
                transformation_ctx = f"write_{table_name}"
            )
            # Alternative for writing to Glue Catalog Table
            # cleaned_df.write.mode("overwrite").saveAsTable(f"`{args['TARGET_DB']}`.target_{table_name}")

        if quarantine_df is not None and quarantine_df.count() > 0:
            glueContext.write_dynamic_frame.from_options(
                frame = glueContext.create_dynamic_frame.fromDF(quarantine_df, glueContext, "quarantine_df"),
                connection_type = "s3",
                connection_options = {"path": f"s3://your-bucket/quarantine/{args['QUARANTINE_TABLE']}/", "partitionKeys": []}, # Replace with your bucket and path
                format = "parquet",
                transformation_ctx = "write_quarantine"
            )
            # Alternative for writing to Glue Catalog Table
            # quarantine_df.write.mode("append").saveAsTable(f"`{args['TARGET_DB']}`.{args['QUARANTINE_TABLE']}")

    job.commit()

if __name__ == "__main__":
    main()
```