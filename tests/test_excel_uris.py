import warnings
warnings.filterwarnings("ignore")

import unittest
import os
import pandas as pd
from owlready2 import default_world, onto_path, Thing, DataProperty, ObjectProperty
onto_path.append('input/ontology_cache/')

if os.path.isfile('./db.sqlite'):
    os.remove('./db.sqlite')
import __main__
__main__.CIDS_URL = 'http://ontology.commonapproach.org/owl/cids_v2.1.owl'
__main__.LOG_FILE = f"./logs/logs.txt"    # Log file used to log errors or warnings that should not stop the processing (e.g. invalid address is found)
if os.path.exists(__main__.LOG_FILE):
    os.remove(__main__.LOG_FILE)
__main__.INPUT_FILE = './tests/ExceltoJSONTemplate-test.xlsx'
__main__.CIDS_URL = 'http://ontology.commonapproach.org/owl/cids_v2.1.owl'
__main__.CONTEXT_PATH = "https://ontology.commonapproach.org/contexts/cidsContext.json"
__main__.REPLACE_PREFIX = 'context_only' # options: 'context_only', 'label_only', 'all', 'none'
from src.utils.contexts import load_context_mapping
__main__.MAPPING_CONTEXT, _ = load_context_mapping(path=__main__.CONTEXT_PATH)

from src.utils import utils

class TestExcelUris(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.input_path=__main__.INPUT_FILE
        cls.xls = pd.ExcelFile(cls.input_path)
  
    @classmethod
    def setUp(cls):
        utils.global_db = {}

    def test_load_uris_theme(self):
        from src.map_data import load_uris
        classes = load_uris(input_path=self.input_path)
        theme_classes = [c for c in classes if c['is_a'] == 'cids.Theme']
        self.assertEqual(len(theme_classes), 3)
        self.assertEqual(theme_classes[0]['ID'], 'http://www.testURI.com/Theme/SDG_A')
        self.assertEqual(theme_classes[0]['cids.hasName'], 'SDG Number A')
        self.assertEqual(theme_classes[0]['cids.hasDescription'], 'Theme description A')
        self.assertEqual(theme_classes[1]['ID'], 'http://www.testURI.com/Theme/SDG_B')
        self.assertEqual(theme_classes[1]['cids.hasName'], 'SDG Number B')
        self.assertEqual(theme_classes[1]['cids.hasDescription'], 'Theme description B')
        self.assertEqual(theme_classes[2]['ID'], 'http://www.testURI.com/Theme/SDG_C')
        self.assertEqual(theme_classes[2]['cids.hasName'], 'SDG Number C')
        self.assertEqual(theme_classes[2]['cids.hasDescription'], 'Theme description C')

    def test_load_uris_outcome(self):
        from src.map_data import load_uris
        classes = load_uris(input_path=self.input_path)
        outcome_classes = [c for c in classes if c['is_a'] == 'cids.Outcome']
        self.assertEqual(len(outcome_classes), 2)
        self.assertEqual(outcome_classes[0]['ID'], 'http://www.testURI.com/Outcome/Outcome1')
        self.assertEqual(outcome_classes[0]['cids.hasName'], 'Outcome One')
        self.assertEqual(outcome_classes[0]['cids.hasDescription'], 'Outcome description one')
        self.assertEqual(len(outcome_classes[0]['cids.forTheme']), 3)
        self.assertIn('http://www.testURI.com/Theme/SDG_A', outcome_classes[0]['cids.forTheme'])
        self.assertIn('http://www.testURI.com/Theme/SDG_B', outcome_classes[0]['cids.forTheme'])
        self.assertIn('http://www.testURI.com/Theme/SDG_C', outcome_classes[0]['cids.forTheme'])

        self.assertEqual(outcome_classes[1]['ID'], 'http://www.testURI.com/Outcome/Outcome2')
        self.assertEqual(outcome_classes[1]['cids.forTheme'], ['http://www.testURI.com/Theme/SDG_B'])
        self.assertEqual(outcome_classes[1]['cids.hasName'], 'Outcome Two')
        self.assertEqual(outcome_classes[1]['cids.hasDescription'], 'Outcome description two')
        
    def test_load_uris_activity(self):
        from src.map_data import load_uris
        classes = load_uris(input_path=self.input_path)
        activity_classes = [c for c in classes if c['is_a'] == 'cids.Activity']
        self.assertEqual(len(activity_classes), 2)
        self.assertEqual(activity_classes[0]['ID'], 'http://www.testURI.com/Activity/Activity1')
        self.assertEqual(activity_classes[0]['cids.hasName'], 'Activity One')
        self.assertEqual(activity_classes[0]['cids.hasOutput'], ['http://www.testURI.com/Output/Output1'])
        self.assertEqual(activity_classes[0]['cids.hasDescription'], 'Activity description one')
        self.assertEqual(activity_classes[1]['ID'], 'http://www.testURI.com/Activity/Activity2')
        self.assertEqual(activity_classes[1]['cids.hasName'], 'Activity Two')
        self.assertEqual(activity_classes[1]['cids.hasOutput'], ['http://www.testURI.com/Output/Output2'])
        self.assertEqual(activity_classes[1]['cids.hasDescription'], 'Activity description two')

if __name__ == '__main__':
    unittest.main(exit=False)
