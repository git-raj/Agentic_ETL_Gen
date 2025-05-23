```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
import sys

def transform_data(customer_df, order_df):
    """
    Transforms the customer and order dataframes.

    Args:
        customer_df: Spark DataFrame representing the customer table.
        order_df: Spark DataFrame representing the order table.

    Returns:
        A dictionary containing the transformed DataFrames, keyed by logical table name.
    """

    output_dataframes = {}

    # ----------------------------------------------------------------------
    # Transformation Logic for Customer Table
    # ----------------------------------------------------------------------

    customer_transformed_df = customer_df.select(
        col("c_custkey").alias("customer_id"),
        col("c_name").alias("customer_name")
    )

    output_dataframes["customer"] = customer_transformed_df

    # ----------------------------------------------------------------------
    # Transformation Logic for Order Table
    # ----------------------------------------------------------------------

    joined_df = order_df.join(customer_df, order_df.o_custkey == customer_df.c_custkey, "inner")

    # Apply filter condition: o_orderdate > '2020-01-01'
    # Assuming there is a column called o_orderdate. If the source does not contain this column, this filter should be removed
    # and the join result should be directly transformed.
    # Ensure o_orderdate is a date type for correct comparison

    if "o_orderdate" in joined_df.columns:
        filtered_df = joined_df.withColumn("o_orderdate", to_date(col("o_orderdate"))) \
                                 .filter(col("o_orderdate") > lit("2020-01-01"))
    else:
        filtered_df = joined_df # No filtering if o_orderdate doesn't exist

    order_transformed_df = filtered_df.select(
        col("o_orderkey").alias("order_id")
    )

    output_dataframes["orders"] = order_transformed_df

    return output_dataframes


def data_quality_checks(spark, df, table_name):
    """Placeholder for data quality checks.  In a real implementation, this would
    perform checks and separate good records from bad, returning both.  For this
    example, it just returns the original dataframe.
    """
    # Example: Check for null values in a specific column (replace 'column_name' and criteria)
    #   valid_df = df.filter(col('column_name').isNotNull())
    #   invalid_df = df.filter(col('column_name').isNull())
    #   return valid_df, invalid_df, valid_df.count() > 0

    # Replace with real implementation later
    return df, None, True # Assuming all records are valid for now

def main():
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_CUSTOMER', 'S3_INPUT_ORDERS', 'S3_OUTPUT_PATH', 'DATABASE_NAME', 'TABLE_PREFIX'])

    glueContext = GlueContext(SparkSession.builder.appName(args['JOB_NAME']).getOrCreate().sparkContext)
    spark = glueContext.spark_session

    # Log basic info
    print(f"Starting Glue Job: {args['JOB_NAME']}")
    print(f"Input Customer Data: {args['S3_INPUT_CUSTOMER']}")
    print(f"Input Orders Data: {args['S3_INPUT_ORDERS']}")
    print(f"Output Path: {args['S3_OUTPUT_PATH']}")
    print(f"Database Name: {args['DATABASE_NAME']}")
    print(f"Table Prefix: {args['TABLE_PREFIX']}")

    # Read data from S3 using GlueContext's data catalog integration (recommended)
    try:
        customer_df = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": [args['S3_INPUT_CUSTOMER']]},
            format="csv",  # Adjust format as needed (e.g., "parquet", "json")
            format_options={"header": True, "inferSchema": True},
            transformation_ctx="customer_df"
        ).toDF()

        order_df = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": [args['S3_INPUT_ORDERS']]},
            format="csv",  # Adjust format as needed
            format_options={"header": True, "inferSchema": True},
            transformation_ctx="order_df"
        ).toDF()
    except Exception as e:
        print(f"Error reading data from S3: {e}")
        raise  # Re-raise to fail the Glue job

    transformed_data = transform_data(customer_df, order_df)

    for table_name, df in transformed_data.items():
        cleaned_df, quarantine_df, record_count_valid = data_quality_checks(spark, df, table_name)

        output_table_name = f"{args['TABLE_PREFIX']}_{table_name}"  # Prefix the table name

        if cleaned_df is not None and record_count_valid:
            try:
                # Write to Glue Data Catalog using DynamicFrames for schema management
                glueContext.write_dynamic_frame.from_options(
                    frame=glueContext.create_dynamic_frame.fromDF(cleaned_df, glueContext, table_name),
                    connection_type="s3",
                    connection_options={"path": f"{args['S3_OUTPUT_PATH']}/{output_table_name}"},
                    format="parquet",  # Recommended for performance
                    transformation_ctx=f"write_{table_name}",
                )

                # Register the table in the Glue Data Catalog
                glueContext.create_table(
                    databaseName=args['DATABASE_NAME'],
                    tableName=output_table_name,
                    catalogId=glueContext.catalog_id, # Add catalog_id
                    output_path=f"{args['S3_OUTPUT_PATH']}/{output_table_name}",
                    data_format="parquet"
                )

                print(f"Successfully wrote table: {output_table_name}")

            except Exception as e:
                print(f"Error writing table {output_table_name}: {e}")
                raise  # Re-raise to fail the Glue job


        if quarantine_df is not None:
            try:
                quarantine_table_name = f"{args['TABLE_PREFIX']}_quarantine_records"
                glueContext.write_dynamic_frame.from_options(
                    frame=glueContext.create_dynamic_frame.fromDF(quarantine_df, glueContext, "quarantine_df"),
                    connection_type="s3",
                    connection_options={"path": f"{args['S3_OUTPUT_PATH']}/{quarantine_table_name}"},
                    format="parquet",
                    transformation_ctx="write_quarantine",
                )

                glueContext.create_table(
                    databaseName=args['DATABASE_NAME'],
                    tableName=quarantine_table_name,
                    catalogId=glueContext.catalog_id, #Add catalog_id
                    output_path=f"{args['S3_OUTPUT_PATH']}/{quarantine_table_name}",
                    data_format="parquet"
                )

                print("Successfully wrote quarantine table")

            except Exception as e:
                print(f"Error writing quarantine table: {e}")
                raise  # Re-raise to fail the Glue job

    print("Glue Job Complete")

if __name__ == "__main__":
    main()
```