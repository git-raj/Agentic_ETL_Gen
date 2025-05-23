def generate_dq_tests(metadata, llm, use_agentic=False):
    """
    Generates Great Expectations + PyTest-based validation tests from metadata.
    Falls back to canned rule generation unless use_agentic is True.
    """
    if use_agentic:
        # Placeholder for LLM-generated expectations
        return llm.invoke([
            {"role": "user", "content": "Generate PyTest+GreatExpectations DQ tests from the following metadata:\n" + metadata.to_csv(index=False)}
        ]).content

    import_text = [
        "import pytest",
        "from great_expectations.dataset import SparkDFDataset"
    ]
    expectations = []

    for _, row in metadata.iterrows():
        validation = str(row.get('Validations', '')).strip()
        column_name = str(row.get('Column Name', '')).strip()

        if not column_name and validation:
            column_name = validation  # fallback

        if not validation or not column_name:
            continue

        rule = validation.lower()

        if rule == "not null":
            expectations.append(
                f'    dataset.expect_column_values_to_not_be_null("{column_name}")'
            )
        elif rule.startswith("range"):
            try:
                bounds = rule.split("(")[1].split(")")[0].split(",")
                min_val, max_val = bounds[0].strip(), bounds[1].strip()
                expectations.append(
                    f'    dataset.expect_column_values_to_be_between("{column_name}", min_value={min_val}, max_value={max_val})'
                )
            except Exception:
                expectations.append(f"    # Invalid range format: {validation}")
        elif rule.startswith("regex"):
            try:
                pattern = rule.split("(")[1].split(")")[0]
                expectations.append(
                    f'    dataset.expect_column_values_to_match_regex("{column_name}", "{pattern}")'
                )
            except Exception:
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
