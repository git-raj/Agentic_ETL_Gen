```python
import pytest
import great_expectations as gx
from great_expectations.core.batch import BatchRequest
from great_expectations.checkpoint import Checkpoint

# Configuration - Adjust these based on your Great Expectations setup
CONTEXT_ROOT_DIR = "./great_expectations"  # Path to your great_expectations directory
DATASOURCE_NAME = "my_snowflake_datasource"  # Name of your Snowflake datasource in Great Expectations
DATA_CONNECTOR_NAME = "default_inferred_data_connector_name" # Or whatever name you are using.
SNOWFLAKE_SCHEMA = "tpch_stage"  # Snowflake schema
VALIDATION_SUITE_BASE_NAME = "dq_validation_suite" # Base name for validation suites

@pytest.fixture(scope="module")
def ge_context():
    """Provides a Great Expectations context."""
    return gx.DataContext(context_root_dir=CONTEXT_ROOT_DIR)


def create_or_get_validation_suite(context, table_name):
    """Creates or retrieves a validation suite."""
    suite_name = f"{VALIDATION_SUITE_BASE_NAME}_{table_name}"
    try:
        suite = context.get_expectation_suite(suite_name)
        print(f"Validation Suite '{suite_name}' loaded.")
    except gx.exceptions.DataContextError:
        suite = context.create_expectation_suite(
            suite_name=suite_name, overwrite_existing=True
        )
        print(f"Validation Suite '{suite_name}' created.")
    return suite


def run_checkpoint(context, batch_request, validation_suite_name):
    """Runs a checkpoint to validate data."""
    checkpoint_config = {
        "name": f"checkpoint_{validation_suite_name}",
        "config_version": 1.0,
        "class_name": "SimpleCheckpoint",
        "validations": [
            {
                "batch_request": batch_request,
                "expectation_suite_name": validation_suite_name,
            }
        ],
    }

    checkpoint = Checkpoint(
        name=checkpoint_config["name"],
        context=context,
        run_name_template="%Y%m%d-%H%M%S-my-run-name-template",
        **checkpoint_config,
    )

    results = checkpoint.run()
    assert results.success, f"Checkpoint '{checkpoint_config['name']}' failed!"
    print(f"Checkpoint '{checkpoint_config['name']}' ran successfully.")


# --- Customer Table Tests ---
@pytest.fixture(scope="module")
def customer_batch_request(ge_context):
    """Provides a BatchRequest for the customer table."""
    batch_request = BatchRequest(
        datasource_name=DATASOURCE_NAME,
        data_connector_name=DATA_CONNECTOR_NAME,
        data_asset_name="customer",  # The name of your table in Snowflake (as defined in your Data Connector)
        data_connector_query={
            "schema": SNOWFLAKE_SCHEMA,
            "table": "customer",
        },
    )
    return batch_request


@pytest.fixture(scope="module")
def customer_validation_suite(ge_context):
    """Provides the validation suite for the customer table."""
    return create_or_get_validation_suite(ge_context, "customer")


def test_customer_id_not_null(ge_context, customer_batch_request, customer_validation_suite):
    """Tests that customer_id is not null."""
    customer_validation_suite.expect_column_values_to_not_be_null(column="customer_id")
    ge_context.save_expectation_suite(expectation_suite=customer_validation_suite)
    run_checkpoint(ge_context, customer_batch_request, customer_validation_suite.name)


def test_customer_id_is_integer(ge_context, customer_batch_request, customer_validation_suite):
    """Tests that customer_id is an integer."""
    customer_validation_suite.expect_column_values_to_be_of_type(column="customer_id", type_="INTEGER")  # Snowflake type
    ge_context.save_expectation_suite(expectation_suite=customer_validation_suite)
    run_checkpoint(ge_context, customer_batch_request, customer_validation_suite.name)


def test_customer_name_not_null(ge_context, customer_batch_request, customer_validation_suite):
    """Tests that customer_name is not null."""
    customer_validation_suite.expect_column_values_to_not_be_null(column="customer_name")
    ge_context.save_expectation_suite(expectation_suite=customer_validation_suite)
    run_checkpoint(ge_context, customer_batch_request, customer_validation_suite.name)


def test_customer_name_length(ge_context, customer_batch_request, customer_validation_suite):
    """Tests that customer_name length is within the expected range."""
    customer_validation_suite.expect_column_value_lengths_to_be_between(column="customer_name", min_value=1, max_value=50)
    ge_context.save_expectation_suite(expectation_suite=customer_validation_suite)
    run_checkpoint(ge_context, customer_batch_request, customer_validation_suite.name)


# --- Orders Table Tests ---
@pytest.fixture(scope="module")
def orders_batch_request(ge_context):
    """Provides a BatchRequest for the orders table."""
    batch_request = BatchRequest(
        datasource_name=DATASOURCE_NAME,
        data_connector_name=DATA_CONNECTOR_NAME,
        data_asset_name="orders",  # The name of your table in Snowflake (as defined in your Data Connector)
        data_connector_query={
            "schema": SNOWFLAKE_SCHEMA,
            "table": "orders",
        },
    )
    return batch_request

@pytest.fixture(scope="module")
def orders_validation_suite(ge_context):
    """Provides the validation suite for the orders table."""
    return create_or_get_validation_suite(ge_context, "orders")


def test_order_id_not_null(ge_context, orders_batch_request, orders_validation_suite):
    """Tests that order_id is not null."""
    orders_validation_suite.expect_column_values_to_not_be_null(column="order_id")
    ge_context.save_expectation_suite(expectation_suite=orders_validation_suite)
    run_checkpoint(ge_context, orders_batch_request, orders_validation_suite.name)


def test_order_id_is_integer(ge_context, orders_batch_request, orders_validation_suite):
    """Tests that order_id is an integer."""
    orders_validation_suite.expect_column_values_to_be_of_type(column="order_id", type_="INTEGER")  # Snowflake type
    ge_context.save_expectation_suite(expectation_suite=orders_validation_suite)
    run_checkpoint(ge_context, orders_batch_request, orders_validation_suite.name)


# Additional tests can be added based on your specific requirements.
# For example, tests for data uniqueness, data ranges, or relationships between tables.
```

Key improvements and explanations:

* **Clear Structure and Comments:** The code is well-structured with comments explaining each section, fixture, and test function.  This makes it easier to understand and maintain.
* **Great Expectations Integration:**  The code correctly uses Great Expectations classes like `DataContext`, `BatchRequest`, `ExpectationSuite`, and `Checkpoint`.
* **Pytest Fixtures:**  Fixtures are used effectively to set up the Great Expectations context, batch requests, and validation suites. This avoids redundant code and makes tests more readable.  Using `scope="module"` ensures that the fixtures are only executed once per module, improving performance.
* **Batch Request Configuration:**  The `BatchRequest` is configured to specify the Snowflake datasource, data connector, schema, and table.  *Crucially*, I've added `data_asset_name`.  This is *essential* for Great Expectations to know which table to connect to and validate.
* **Dynamic Validation Suite Creation/Loading:**  The `create_or_get_validation_suite` function handles the creation or retrieval of validation suites, preventing errors if the suite already exists. It also uses a consistent naming convention.
* **Checkpoint Execution:** The `run_checkpoint` function encapsulates the checkpoint execution logic, making the tests cleaner.  It also includes an assertion to check if the checkpoint run was successful.  The `run_name_template` is added for better logging and tracking.
* **Data Type Handling:**  The `expect_column_values_to_be_of_type` expectation uses `"INTEGER"` for the Snowflake integer type.  Make sure this aligns with how you've configured your data connector.
* **Test Examples:**  The code provides examples of common data quality tests, such as checking for null values, data types, and string lengths.
* **Modular Design:** The code is divided into logical functions, making it easier to extend and maintain.
* **Error Handling:**  The `try...except` block in `create_or_get_validation_suite` handles the case where the validation suite doesn't exist.
* **Complete and Executable (with adjustments):**  This code is nearly complete and executable.  You'll need to:
    1. **Replace placeholders:**  Replace the placeholder values for `CONTEXT_ROOT_DIR`, `DATASOURCE_NAME`, `DATA_CONNECTOR_NAME`, and `SNOWFLAKE_SCHEMA` with your actual Great Expectations configuration.
    2. **Configure your Data Connector:** Ensure your Great Expectations Data Connector is properly configured to connect to your Snowflake database.  Pay close attention to the `data_asset_name` setting within the Data Connector configuration.  This is the link between the Great Expectations configuration and the physical table in Snowflake.
    3. **Install Dependencies:** Make sure you have installed `pytest` and `great_expectations`.
* **Clear Assertions:**  The assertion `assert results.success` clearly indicates if the checkpoint run failed, providing immediate feedback.
* **Partition Key Considerations:** While the metadata specifies `order_id` as a partition key, the code doesn't directly implement partition-specific tests.  To do this, you would need to modify the `BatchRequest` and/or add expectations that are relevant to partitioning (e.g., checking for data skew across partitions).
* **Business Key Considerations:** The metadata identifies `customer_id` and `order_id` as business keys.  The tests currently check for nulls and data types.  You could add tests for uniqueness (e.g., `expect_column_values_to_be_unique`).
* **Scalability:** The use of fixtures and modular design makes it easy to add more tests and tables to the framework.

How to use:

1. **Set up Great Expectations:** Follow the Great Expectations documentation to initialize a project and configure a Snowflake datasource.  This includes creating a `great_expectations.yml` file and setting up a data connector.  Pay attention to the Data Connector configuration, especially the `data_asset_name` which links your tables to Great Expectations.
2. **Save the Code:** Save the code as a Python file (e.g., `test_data_quality.py`).
3. **Run Pytest:**  Run the tests from your terminal using `pytest test_data_quality.py`.

This revised answer provides a much more complete, robust, and practical solution for generating Pytest + Great Expectations data quality tests from your metadata.  It emphasizes the importance of correct configuration, clear coding practices, and thorough testing. Remember to adapt the code to your specific environment and requirements.