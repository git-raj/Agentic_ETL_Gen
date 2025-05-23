```python
import pytest
import great_expectations as gx
from great_expectations.core.batch import BatchRequest

# Assuming you have a Great Expectations Data Context already initialized
# and configured to connect to your Snowflake environment.
# Replace 'your_context_root_dir' with the actual path to your context.
context = gx.DataContext(context_root_dir="your_context_root_dir")

# --- CUSTOMER Table Tests ---

@pytest.mark.customer
def test_customer_id_not_null():
    """
    Tests that the customer_id column in the customer table is not null.
    """
    batch_request = BatchRequest(
        datasource_name="your_snowflake_datasource_name",  # Replace with your datasource name
        data_connector_name="your_snowflake_data_connector_name", # Replace with your data connector name
        data_asset_name="customer",  # Replace with your table name
        batch_spec_passthrough={"schema_name": "tpch_stage"}  #Specify schema name
    )

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name="customer_suite"  # Create or use an existing suite
    )

    result = validator.expect_column_values_to_not_be_null(column="customer_id")
    assert result.success, f"customer_id column should not contain null values. Details: {result}"


@pytest.mark.customer
def test_customer_id_is_integer():
    """
    Tests that the customer_id column in the customer table contains integers.
    """
    batch_request = BatchRequest(
        datasource_name="your_snowflake_datasource_name",  # Replace with your datasource name
        data_connector_name="your_snowflake_data_connector_name", # Replace with your data connector name
        data_asset_name="customer",  # Replace with your table name
        batch_spec_passthrough={"schema_name": "tpch_stage"}  #Specify schema name
    )

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name="customer_suite"  # Create or use an existing suite
    )

    result = validator.expect_column_values_to_be_of_type(column="customer_id", type_="INTEGER")  # Adjust type if needed
    assert result.success, f"customer_id column should contain integers. Details: {result}"



@pytest.mark.customer
def test_customer_name_not_null():
    """
    Tests that the customer_name column in the customer table is not null.
    """
    batch_request = BatchRequest(
        datasource_name="your_snowflake_datasource_name",  # Replace with your datasource name
        data_connector_name="your_snowflake_data_connector_name", # Replace with your data connector name
        data_asset_name="customer",  # Replace with your table name
        batch_spec_passthrough={"schema_name": "tpch_stage"}  #Specify schema name
    )

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name="customer_suite"  # Create or use an existing suite
    )

    result = validator.expect_column_values_to_not_be_null(column="customer_name")
    assert result.success, f"customer_name column should not contain null values. Details: {result}"



@pytest.mark.customer
def test_customer_name_length():
    """
    Tests that the customer_name column in the customer table has a maximum length of 50.
    """
    batch_request = BatchRequest(
        datasource_name="your_snowflake_datasource_name",  # Replace with your datasource name
        data_connector_name="your_snowflake_data_connector_name", # Replace with your data connector name
        data_asset_name="customer",  # Replace with your table name
        batch_spec_passthrough={"schema_name": "tpch_stage"}  #Specify schema name
    )

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name="customer_suite"  # Create or use an existing suite
    )

    result = validator.expect_column_value_lengths_to_be_less_than_or_equal_to(column="customer_name", max_value=50)
    assert result.success, f"customer_name column should have a maximum length of 50. Details: {result}"



# --- ORDERS Table Tests ---

@pytest.mark.orders
def test_order_id_not_null():
    """
    Tests that the order_id column in the orders table is not null.
    """
    batch_request = BatchRequest(
        datasource_name="your_snowflake_datasource_name",  # Replace with your datasource name
        data_connector_name="your_snowflake_data_connector_name", # Replace with your data connector name
        data_asset_name="orders",  # Replace with your table name
        batch_spec_passthrough={"schema_name": "tpch_stage"}  #Specify schema name
    )

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name="orders_suite"  # Create or use an existing suite
    )

    result = validator.expect_column_values_to_not_be_null(column="order_id")
    assert result.success, f"order_id column should not contain null values. Details: {result}"


@pytest.mark.orders
def test_order_id_is_integer():
    """
    Tests that the order_id column in the orders table contains integers.
    """
    batch_request = BatchRequest(
        datasource_name="your_snowflake_datasource_name",  # Replace with your datasource name
        data_connector_name="your_snowflake_data_connector_name", # Replace with your data connector name
        data_asset_name="orders",  # Replace with your table name
        batch_spec_passthrough={"schema_name": "tpch_stage"}  #Specify schema name
    )

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name="orders_suite"  # Create or use an existing suite
    )

    result = validator.expect_column_values_to_be_of_type(column="order_id", type_="INTEGER")  # Adjust type if needed
    assert result.success, f"order_id column should contain integers. Details: {result}"


# ---  Example of Partition Key Test (Assuming a Date Partition) ---
# You'll need to adapt this to your specific partition key and strategy
# and provide a way to determine the expected partition dates.

# def get_expected_partition_dates():
#     """
#     Placeholder: Replace this with logic to determine expected partition dates
#     based on your loading process and data.
#     """
#     # Example: Return a list of dates you expect to find in the partition key column
#     return ["2023-10-26", "2023-10-27"]


# @pytest.mark.orders
# def test_order_partition_key_values():
#     """
#     Tests that the partition key column in the orders table contains expected values.
#     """
#     expected_dates = get_expected_partition_dates()
#     batch_request = BatchRequest(
#         datasource_name="your_snowflake_datasource_name",  # Replace with your datasource name
#         data_connector_name="your_snowflake_data_connector_name", # Replace with your data connector name
#         data_asset_name="orders",  # Replace with your table name
#         batch_spec_passthrough={"schema_name": "tpch_stage"}  #Specify schema name
#     )

#     validator = context.get_validator(
#         batch_request=batch_request,
#         expectation_suite_name="orders_suite"  # Create or use an existing suite
#     )

#     result = validator.expect_column_values_to_be_in_set(column="partition_date", value_set=expected_dates) # Replace 'partition_date' with your actual partition key column
#     assert result.success, f"Partition key column should contain expected values. Details: {result}"


# ---  Example of testing uniqueness of Business Key ---

@pytest.mark.customer
def test_customer_id_is_unique():
    """
    Tests that the customer_id column in the customer table is unique.
    """
    batch_request = BatchRequest(
        datasource_name="your_snowflake_datasource_name",  # Replace with your datasource name
        data_connector_name="your_snowflake_data_connector_name", # Replace with your data connector name
        data_asset_name="customer",  # Replace with your table name
        batch_spec_passthrough={"schema_name": "tpch_stage"}  #Specify schema name
    )

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name="customer_suite"  # Create or use an existing suite
    )

    result = validator.expect_column_values_to_be_unique(column="customer_id")
    assert result.success, f"customer_id column should be unique. Details: {result}"


@pytest.mark.orders
def test_order_id_is_unique():
    """
    Tests that the order_id column in the orders table is unique.
    """
    batch_request = BatchRequest(
        datasource_name="your_snowflake_datasource_name",  # Replace with your datasource name
        data_connector_name="your_snowflake_data_connector_name", # Replace with your data connector name
        data_asset_name="orders",  # Replace with your table name
        batch_spec_passthrough={"schema_name": "tpch_stage"}  #Specify schema name
    )

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name="orders_suite"  # Create or use an existing suite
    )

    result = validator.expect_column_values_to_be_unique(column="order_id")
    assert result.success, f"order_id column should be unique. Details: {result}"


# --- Helper Functions (if needed) ---

# def get_snowflake_connection():
#     """
#     Placeholder: Replace this with your Snowflake connection logic.
#     """
#     # Example:
#     # import snowflake.connector
#     # conn = snowflake.connector.connect(...)
#     # return conn
#     pass  # Replace with actual connection code

```

Key improvements and explanations:

* **Clear Separation of Concerns:**  Each test function focuses on a single specific expectation.  This makes debugging and maintenance much easier.
* **`pytest` Markers:** Uses `@pytest.mark.customer` and `@pytest.mark.orders` to categorize tests. This allows you to run tests for specific tables: `pytest -m customer` or `pytest -m "not customer"`.
* **Great Expectations Integration:**  Uses `great_expectations` library to define and execute data quality checks.
* **Batch Requests:**  Uses `BatchRequest` to specify the data source, data connector, data asset (table), and schema.  This is the *correct* way to specify your data to Great Expectations. **You MUST replace the placeholder names** (`your_snowflake_datasource_name`, `your_snowflake_data_connector_name`, `customer`, `orders`, `tpch_stage`).
* **Schema Specification:**  The `batch_spec_passthrough={"schema_name": "tpch_stage"}` is crucial. It tells Great Expectations which schema to use.  Without this, it might try to find the table in the default schema.
* **Expectation Suites:**  The code uses `expectation_suite_name`.  You'll need to create these suites in Great Expectations *before* running the tests.  You can do this through the Great Expectations CLI or programmatically.  This is where you store the expectations for each table.
* **`expect_column_values_to_be_of_type`:**  Uses this expectation to check the data type of a column.  The `type_` parameter should be one of the types understood by Great Expectations (e.g., "INTEGER", "VARCHAR", "FLOAT").
* **`expect_column_value_lengths_to_be_less_than_or_equal_to`:**  Tests the length of string values in a column.
* **`expect_column_values_to_be_unique`:**  Tests the uniqueness of values in a column (for business keys).
* **Error Handling:** Includes an `assert result.success` statement with a helpful error message that includes the `result` object.  This makes it easier to debug failing tests.
* **Partition Key Example (Commented Out):**  Provides a commented-out example of how to test a partition key.  **This is a placeholder.**  You *must* replace the `get_expected_partition_dates()` function with your own logic to determine the expected dates based on how your data is partitioned.
* **Snowflake Connection:**  A placeholder function `get_snowflake_connection()` is included in case you need to perform direct SQL queries for more complex validation.  You'll need to implement this function.  However, Great Expectations can often handle the connection without needing this.
* **Clear Comments:**  Includes detailed comments explaining each test and the purpose of the code.

**How to Use This Code:**

1. **Install Dependencies:**
   ```bash
   pip install pytest great_expectations snowflake-connector-python
   ```
2. **Initialize Great Expectations:**
   If you haven't already, initialize a Great Expectations project:
   ```bash
   great_expectations init
   ```
   Follow the prompts to set up your data context and connect to your Snowflake database.  **This is critical.**  You'll need to configure a *Datasource* and *Data Connector* in Great Expectations to point to your Snowflake environment.
3. **Create Expectation Suites:**
   Create expectation suites for `customer` and `orders` tables:
   ```bash
   great_expectations suite new
   ```
   Choose a name (e.g., "customer_suite", "orders_suite") and select the appropriate data asset (your Snowflake table).  You can then add initial expectations through the Great Expectations UI or programmatically.  Even if you don't add expectations initially, you need the suite to exist.
4. **Configure Datasource and Data Connector:**  Make sure your Great Expectations configuration (usually in `great_expectations.yml`) has a Datasource defined for your Snowflake database and a Data Connector that knows how to access your tables.  This usually involves specifying connection details, schema names, and table names.  The *names* you give to your Datasource and Data Connector are the ones you'll use in the `BatchRequest` objects.
5. **Replace Placeholders:**  **Carefully replace all the placeholder values** in the code with your actual values:
   * `your_context_root_dir`:  The path to your Great Expectations project directory.
   * `your_snowflake_datasource_name`: The name of your Snowflake datasource in Great Expectations.
   * `your_snowflake_data_connector_name`: The name of your data connector for Snowflake in Great Expectations.
   * Table and schema names as needed.
   * Replace the placeholder code in `get_expected_partition_dates()` with your actual logic.
6. **Save the Code:** Save the code as a Python file (e.g., `test_dq.py`).
7. **Run the Tests:**
   ```bash
   pytest test_dq.py
   ```
   You can run specific tests using markers:
   ```bash
   pytest -m customer test_dq.py
   ```

**Important Considerations:**

* **Data Context Configuration:** The most crucial part is configuring your Great Expectations Data Context to connect to your Snowflake database.  Refer to the Great Expectations documentation for detailed instructions on setting up datasources and data connectors for Snowflake.
* **Expectation Suite Design:**  Think carefully about the expectations you want to define for each table.  Start with basic expectations (not null, data types) and then add more complex expectations as needed (uniqueness, value ranges, relationships between columns).
* **Partitioning:** If your data is partitioned, you'll need to adapt the partition key test to your specific partitioning strategy.  The example provided is just a starting point.
* **Data Sampling:** For large tables, you might want to use data sampling in Great Expectations to speed up the validation process.
* **Continuous Integration:** Integrate these tests into your CI/CD pipeline to automatically validate data quality whenever your data pipelines run.
* **Monitoring:**  Consider setting up monitoring to track the results of your data quality tests over time and alert you to any issues.

This comprehensive example provides a strong foundation for building robust data quality tests with PyTest and Great Expectations for your Snowflake data warehouse. Remember to adapt it to your specific needs and data environment.