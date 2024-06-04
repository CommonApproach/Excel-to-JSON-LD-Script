import warnings
warnings.filterwarnings("ignore")

import unittest
import os
import pandas as pd
import json
import sys

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
JSON_OUTPATH = './tests/output.json'

class TestExcelContextContextOnly(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.input_path=__main__.INPUT_FILE
        cls.json_out_path=JSON_OUTPATH
        cls.xls = pd.ExcelFile(cls.input_path)


    def test_load_organization_replace_prefix_context_only(self):
        from src.utils.contexts import load_context_mapping
        __main__.MAPPING_CONTEXT, _ = load_context_mapping(path=__main__.CONTEXT_PATH)
        __main__.REPLACE_PREFIX = 'context_only'
        from src.generators.organization import generate_organization
        from src.map_data import load_indicators
        from src.map_data import load_uris

        from src.utils.utils import global_db
        global_db = {}
        _=load_uris(input_path=__main__.INPUT_FILE)
        _ = generate_organization(input_path=__main__.INPUT_FILE)
        _=load_indicators(input_path=__main__.INPUT_FILE)


        from src.utils.utils import db_to_json
        from src.utils.utils import global_db
        global_db_hold = global_db.copy()
        objects = db_to_json(dict_db=global_db_hold)
        orgs = [o for o in objects if o['@type'] == 'cids:Organization']
        self.assertEqual(len(orgs), 1)
        org = orgs[0]
        self.assertIsNone(org.get('org:hasLegalName'))
        self.assertIsNotNone(org.get('hasLegalName'))
        self.assertEqual(org['hasLegalName'], 'Test Organization Name')


if __name__ == '__main__':
    unittest.main(exit=False)
