import unittest
from src import lineage_tasks
import pandas as pd
import json

class TestLineageTasks(unittest.TestCase):

    def test_generate_lineage_json_empty_metadata(self):
        metadata = pd.DataFrame()
        llm = None
        result = lineage_tasks.generate_lineage_json(metadata, llm)
        self.assertEqual(json.loads(result), {'codeGraph': {'nodes': [], 'edges': []}, 'sourceProperties': [], 'targetProperties': [], 'script': {'codeFormat': 'python', 'placeholder': '# Transformation logic not provided'}})

    def test_generate_lineage_json_valid_mapping(self):
        metadata = pd.DataFrame({
            'Business Rule / Expression': ['target_id = source_id']
        })
        llm = None
        result = lineage_tasks.generate_lineage_json(metadata, llm)
        result_json = json.loads(result)
        self.assertEqual(len(result_json['codeGraph']['nodes']), 2)
        self.assertEqual(len(result_json['codeGraph']['edges']), 1)
        self.assertEqual(result_json['codeGraph']['nodes'][0]['name'], 'source_id')
        self.assertEqual(result_json['codeGraph']['nodes'][1]['name'], 'target')
        self.assertEqual(result_json['codeGraph']['edges'][0]['label'], 'target_id = source_id')
        self.assertEqual(result_json['codeGraph']['nodes'][0]['type'], 'Column')
        self.assertEqual(result_json['codeGraph']['nodes'][1]['type'], 'Column')
        self.assertEqual(result_json['codeGraph']['nodes'][0]['parent']['type'], 'Table')
        self.assertEqual(result_json['codeGraph']['nodes'][1]['parent']['type'], 'Table')
        self.assertEqual(result_json['codeGraph']['nodes'][0]['parent']['name'], 'source')
        self.assertEqual(result_json['codeGraph']['nodes'][1]['parent']['name'], 'target')

if __name__ == '__main__':
    unittest.main()
