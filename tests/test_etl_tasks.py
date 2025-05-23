import unittest
from src import etl_tasks
import pandas as pd

class TestETLTasks(unittest.TestCase):

    def test_generate_etl_code_empty_source_metadata(self):
        source_metadata = pd.DataFrame()
        target_metadata = pd.DataFrame({'Target Table Name': ['target_table']})
        target_platform = "Databricks"
        llm = None
        result = etl_tasks.generate_etl_code(source_metadata, target_metadata, target_platform, llm)
        self.assertEqual(result, "Error: Source metadata is empty.")

    def test_generate_etl_code_empty_target_metadata(self):
        source_metadata = pd.DataFrame({'Source Table Name': ['source_table']})
        target_metadata = pd.DataFrame()
        target_platform = "Databricks"
        llm = None
        result = etl_tasks.generate_etl_code(source_metadata, target_metadata, target_platform, llm)
        self.assertEqual(result, "Error: Target metadata is empty.")

    def test_generate_etl_code_missing_source_table_name(self):
        source_metadata = pd.DataFrame({'Source Column Name': ['source_col']})
        target_metadata = pd.DataFrame({'Target Table Name': ['target_table']})
        target_platform = "Databricks"
        llm = None
        result = etl_tasks.generate_etl_code(source_metadata, target_metadata, target_platform, llm)
        self.assertEqual(result, "Error: 'Source Table Name' column not found in source metadata.")

    def test_generate_etl_code_missing_target_table_name(self):
        source_metadata = pd.DataFrame({'Source Table Name': ['source_table']})
        target_metadata = pd.DataFrame({'Target Column Name': ['target_col']})
        target_platform = "Databricks"
        llm = None
        result = etl_tasks.generate_etl_code(source_metadata, target_metadata, target_platform, llm)
        self.assertEqual(result, "Error: 'Target Table Name' column not found in target metadata.")

    def test_generate_etl_code_no_source_table_name(self):
        source_metadata = pd.DataFrame({'Source Table Name': [None]})
        target_metadata = pd.DataFrame({'Target Table Name': ['target_table']})
        target_platform = "Databricks"
        llm = None
        result = etl_tasks.generate_etl_code(source_metadata, target_metadata, target_platform, llm)
        self.assertEqual(result, "Error: No non-null 'Source Table Name' found in source metadata.")

    def test_generate_etl_code_no_target_table_name(self):
        source_metadata = pd.DataFrame({'Source Table Name': ['source_table']})
        target_metadata = pd.DataFrame({'Target Table Name': [None]})
        target_platform = "Databricks"
        llm = None
        result = etl_tasks.generate_etl_code(source_metadata, target_metadata, target_platform, llm)
        self.assertEqual(result, "Error: No non-null 'Target Table Name' found in target metadata.")

    def test_generate_etl_code_success(self):
        source_metadata = pd.DataFrame({'Source Table Name': ['source_table']})
        target_metadata = pd.DataFrame({'Target Table Name': ['target_table'], 'Target Column Name': ['target_col'], 'Transformation Logic': ['Direct mapping from source_col']})
        target_platform = "Databricks"
        llm = None
        result = etl_tasks.generate_etl_code(source_metadata, target_metadata, target_platform, llm)
        self.assertIn("spark = SparkSession.builder.appName(\"ETL Job\").getOrCreate()", result)
        self.assertIn('target_df = target_df.withColumn("target_col", source_df["source_col"])', result)
        self.assertIn('target_df.write.format("delta").mode("overwrite").saveAsTable("target_table")', result)

if __name__ == '__main__':
    unittest.main()
