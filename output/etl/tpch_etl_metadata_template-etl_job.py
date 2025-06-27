```python
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit
from snowflake.snowpark.types import StructType, StructField, IntegerType, StringType, DateType
import snowflake.connector

def transform_data(customer_df, order_df):
    """
    Transforms data from customer and order DataFrames.

    Args:
        customer_df (snowflake.snowpark.DataFrame): DataFrame representing the customer table.
        order_df (snowflake.snowpark.DataFrame): DataFrame representing the order table.

    Returns:
        dict: A dictionary containing the transformed DataFrames, keyed by logical table name.
    """

    transformed_data = {}

    # Customer Transformation
    customer_transformed_df = customer_df.select(
        col("c_custkey").alias("customer_id"),
        col("c_name").alias("customer_name")
    )
    transformed_data["customer"] = customer_transformed_df

    # Orders Transformation
    filtered_order_df = order_df.filter(col("o_orderdate") > '2020-01-01')
    joined_df = filtered_order_df.join(customer_df, col("o_custkey") == col("c_custkey"), "inner")
    orders_transformed_df = joined_df.select(
        col("o_orderkey").alias("order_id")
    )
    transformed_data["orders"] = orders_transformed_df

    return transformed_data


def data_quality_checks(session, df, table_name):
    """
    Performs data quality checks on a Snowpark DataFrame.

    Args:
        session (snowflake.snowpark.Session): The Snowpark session object.
        df (snowflake.snowpark.DataFrame): The input Snowpark DataFrame.
        table_name (str): The name of the table being checked.

    Returns:
        tuple: A tuple containing the cleaned DataFrame, quarantine DataFrame, and a boolean indicating record count validity.
    """

    cleaned_df = df
    quarantine_df = session.create_dataframe([], schema=df.schema)
    record_count_valid = True

    # Not Null Check and Quarantine
    null_check_columns = df.columns
    null_condition = " OR ".join([f"col('{c}').isNull()" for c in null_check_columns])

    invalid_records = df.filter(null_condition)
    valid_records = df.filter(~eval(null_condition))

    quarantine_df = quarantine_df.union(invalid_records)
    cleaned_df = valid_records

    # Record Count Check
    record_count = cleaned_df.count()
    expected_record_count = 0

    if record_count <= expected_record_count:
        print(f"ERROR: Table {table_name} has {record_count} records, expected > {expected_record_count}")
        record_count_valid = False
    else:
        print(f"INFO: Table {table_name} has {record_count} records, which meets the expected threshold > {expected_record_count}")

    return cleaned_df, quarantine_df, record_count_valid


def main():
    # Use Snowpark Session
    session = Session.builder.configs(snowflake.connector.connection_config.SnowflakeURL()).create()
    print(session.sql('select current_warehouse(), current_database(), current_schema()').collect())


    # Replace with your actual table names or file paths in Snowflake
    customer_table = "CUSTOMER_TABLE"  # Example: 'MY_DATABASE.MY_SCHEMA.CUSTOMER'
    orders_table = "ORDERS_TABLE" # Example: 'MY_DATABASE.MY_SCHEMA.ORDERS'
    target_database = "TARGET_DATABASE" # Example: 'TARGET_DB'
    target_schema = "TARGET_SCHEMA" #Example: 'TARGET_SCH'
    quarantine_table = f"{target_database}.{target_schema}.QUARANTINE_RECORDS"

    # Read data from Snowflake tables
    try:
        customer_df = session.table(customer_table)
        order_df = session.table(orders_table)
    except Exception as e:
        print(f"Error reading tables: {e}")
        session.close()
        return

    transformed_data = transform_data(customer_df, order_df)

    for table_name, df in transformed_data.items():
        cleaned_df, quarantine_df, record_count_valid = data_quality_checks(session, df, table_name)

        target_table_name = f"{target_database}.{target_schema}.TARGET_{table_name.upper()}"


        if cleaned_df is not None and record_count_valid:
            try:
                cleaned_df.write.mode("overwrite").save_as_table(target_table_name)
                print(f"Successfully wrote cleaned data to {target_table_name}")
            except Exception as e:
                print(f"Error writing cleaned data to {target_table_name}: {e}")

        if quarantine_df is not None and quarantine_df.count() > 0:
             try:
                quarantine_df.write.mode("append").save_as_table(quarantine_table)
                print(f"Successfully wrote quarantine data to {quarantine_table}")
             except Exception as e:
                print(f"Error writing quarantine data to {quarantine_table}: {e}")

    session.close()


if __name__ == "__main__":
    main()
```