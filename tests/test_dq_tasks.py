import unittest
from src import dq_tasks
import pandas as pd

class TestDQTasks(unittest.TestCase):

    def test_generate_dq_tests_empty_metadata(self):
        metadata = pd.DataFrame()
        llm = None
        result = dq_tasks.generate_dq_tests(metadata, llm)
        self.assertIn("import pytest", result)

    def test_generate_dq_tests_not_null(self):
        metadata = pd.DataFrame({'Column Name': ['col1'], 'Validations': ['Not Null']})
        llm = None
        result = dq_tasks.generate_dq_tests(metadata, llm)
        self.assertIn('dataset.expect_column_values_to_not_be_null("col1")', result)

    def test_generate_dq_tests_range(self):
        metadata = pd.DataFrame({'Column Name': ['col1'], 'Validations': ['Range(1, 10)']})
        llm = None
        result = dq_tasks.generate_dq_tests(metadata, llm)
        self.assertIn('dataset.expect_column_values_to_be_between("col1", min_value=1, max_value=10)', result)

    def test_generate_dq_tests_regex(self):
        metadata = pd.DataFrame({'Column Name': ['col1'], 'Validations': ['Regex(^[a-zA-Z]+$)']})
        llm = None
        result = dq_tasks.generate_dq_tests(metadata, llm)
        self.assertIn('dataset.expect_column_values_to_match_regex("col1", "^[a-za-z]+$")', result)

    def test_generate_dq_tests_unsupported(self):
        metadata = pd.DataFrame({'Column Name': ['col1'], 'Validations': ['Unsupported']})
        llm = None
        result = dq_tasks.generate_dq_tests(metadata, llm)
        self.assertIn('# Unsupported validation rule: Unsupported', result)

if __name__ == '__main__':
    unittest.main()
