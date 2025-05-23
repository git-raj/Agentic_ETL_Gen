import unittest
from src import agents
import pandas as pd

class TestAgents(unittest.TestCase):

    def test_agents_run_etl(self):
        llm = None
        test_agents = agents.Agents(llm)
        source_metadata_df = pd.DataFrame({'Source Table Name': ['source_table']})
        target_metadata_df = pd.DataFrame({'Target Table Name': ['target_table'], 'Target Column Name': ['target_col'], 'Transformation Logic': ['Direct mapping from source_col']})
        result = test_agents.run(source_metadata_df=source_metadata_df, target_metadata_df=target_metadata_df)
        self.assertIn("etl", result)

    def test_agents_run_lineage(self):
        llm = None
        test_agents = agents.Agents(llm)
        mapping_metadata_df = pd.DataFrame({'Business Rule / Expression': ['target_id = source_id']})
        result = test_agents.run(mapping_metadata_df=mapping_metadata_df)
        self.assertIn("lineage", result)

    def test_agents_run_dq(self):
        llm = None
        test_agents = agents.Agents(llm)
        target_metadata_df = pd.DataFrame({'Column Name': ['col1'], 'Validations': ['Not Null']})
        result = test_agents.run(target_metadata_df=target_metadata_df)
        self.assertIn("dq", result)

if __name__ == '__main__':
    unittest.main()
