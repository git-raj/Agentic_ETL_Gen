def generate_data_quality_tests(metadata, llm_option):
    """
    Generates Great Expectations + PyTest-based validation tests based on the provided metadata.
    """
    import_text = [
        "import pytest",
        "from great_expectations.dataset import SparkDFDataset"
    ]

    expectations = []

    for _, row in metadata.iterrows():
        validation = row.get('Validations')
        reject_handling = row.get('Reject Handling', '')
        column_name = row.get('Column Name') or validation  # fallback to validation field

        if validation and isinstance(validation, str):
            if validation.lower() == "not null":
                expectations.append(
                    f'    dataset.expect_column_values_to_not_be_null("{column_name}")'
                )
            elif validation.lower().startswith("range"):
                # Example format: Range(1,100)
                try:
                    bounds = validation.split("(")[1].split(")")[0].split(",")
                    min_val, max_val = bounds[0].strip(), bounds[1].strip()
                    expectations.append(
                        f'    dataset.expect_column_values_to_be_between("{column_name}", min_value={min_val}, max_value={max_val})'
                    )
                except:
                    expectations.append(f"    # Invalid range format: {validation}")
            elif validation.lower().startswith("regex"):
                try:
                    pattern = validation.split("(")[1].split(")")[0]
                    expectations.append(
                        f'    dataset.expect_column_values_to_match_regex("{column_name}", "{pattern}")'
                    )
                except:
                    expectations.append(f"    # Invalid regex format: {validation}")
            else:
                expectations.append(f"    # Unsupported validation rule: {validation}")

    test_code = f"""
{chr(10).join(import_text)}

def load_data():
    # TODO: Replace with your actual Spark dataframe loading logic
    return ...

def test_data_quality():
    data = load_data()
    dataset = SparkDFDataset(data)
{chr(10).join(expectations)}
    results = dataset.validate()
    assert results["success"]

if __name__ == "__main__":
    test_data_quality()
"""

    return test_code
