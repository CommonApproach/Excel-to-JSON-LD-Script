import warnings
warnings.filterwarnings("ignore")

import unittest
import os
import pandas as pd

if os.path.isfile('./db.sqlite'):
    os.remove('./db.sqlite')
import __main__
__main__.CIDS_URL = 'http://ontology.commonapproach.org/owl/cids_v2.1.owl'
# from __main__ import LOG_FILE, CONTEXT_PATH, MAPPING_CONTEXT, REPLACE_PREFIX
__main__.LOG_FILE = f"./logs/logs.txt"    # Log file used to log errors or warnings that should not stop the processing (e.g. invalid address is found)
if os.path.exists(__main__.LOG_FILE):
    os.remove(__main__.LOG_FILE)
__main__.INPUT_FILE = './tests/ExceltoJSONTemplate-test.xlsx'
__main__.CIDS_URL = 'http://ontology.commonapproach.org/owl/cids_v2.1.owl'
__main__.CONTEXT_PATH = "https://ontology.commonapproach.org/contexts/cidsContext.json"
__main__.REPLACE_PREFIX = 'context_only' # options: 'context_only', 'label_only', 'all', 'none'
from src.utils.contexts import load_context_mapping
__main__.MAPPING_CONTEXT, _ = load_context_mapping(path=__main__.CONTEXT_PATH)

from src.utils.namespaces import prop_ranges_preset
from src.utils import utils


class TestExcelOrganizations(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.input_path=__main__.INPUT_FILE
        cls.xls = pd.ExcelFile(cls.input_path)
  
    @classmethod
    def setUp(cls):
        utils.global_db = {}


    def test_load_organization(self):
        from src.generators.organization import generate_organization
        _ = generate_organization(input_path=self.input_path)
        org = utils.global_db['http://www.testURI.com/ABC123']
        self.assertEqual(org['is_a'], 'cids.Organization')
        self.assertEqual(org['ID'], 'http://www.testURI.com/ABC123')
        self.assertEqual(org['org.hasLegalName'], ['Organization Name Value'])
        self.assertEqual(org['cids.hasImpactModel'], ['http://www.testURI.com/main-impactmodel'])


    def test_load_impactmodel(self):
        from src.generators.organization import generate_organization
        _ = generate_organization(input_path=self.input_path)
        im = utils.global_db['http://www.testURI.com/main-impactmodel']
        self.assertEqual(im['ID'], 'http://www.testURI.com/main-impactmodel')
        self.assertEqual(im['is_a'], 'cids.LogicModel')
        self.assertEqual(im['cids.hasName'], ['Impact Model'])
        self.assertEqual(im['cids.hasProgram'], ['http://www.testURI.com/loan-program'])


    def test_load_program(self):
        from src.generators.organization import generate_organization
        _ = generate_organization(input_path=self.input_path)
        program = utils.global_db['http://www.testURI.com/loan-program']
        self.assertEqual(program['ID'], 'http://www.testURI.com/loan-program')
        self.assertEqual(program['is_a'], 'cids.Program')
        self.assertEqual(program['cids.hasName'], ['Loans Program'])
        self.assertEqual(program['cids.hasService'], ['http://www.testURI.com/loan-service'])

    def test_load_service(self):
        from src.generators.organization import generate_organization
        _ = generate_organization(input_path=self.input_path)
        service = utils.global_db['http://www.testURI.com/loan-service']
        self.assertEqual(service['ID'], 'http://www.testURI.com/loan-service')
        self.assertEqual(service['is_a'], 'cids.Service')
        self.assertEqual(service['cids.hasName'], 'Loans Service')
        self.assertEqual(service['act.hasSubActivity'], ['http://www.testURI.com/loan-application-review'])

    def test_load_activity(self):
        from src.generators.organization import generate_organization
        _ = generate_organization(input_path=self.input_path)
        activity = utils.global_db['http://www.testURI.com/loan-application-review']
        self.assertEqual(activity['ID'], 'http://www.testURI.com/loan-application-review')
        self.assertEqual(activity['is_a'], 'cids.Activity')
        self.assertEqual(activity['cids.hasName'], 'Loans Application review')
        self.assertEqual(activity['act.subActivityOf'], ['http://www.testURI.com/loan-service'])
        self.assertEqual(activity['cids.hasOutput'], ['http://www.testURI.com/loan-application-review-output'])

    def test_load_outcome(self):
        from src.generators.organization import generate_organization
        _ = generate_organization(input_path=self.input_path)
        output = utils.global_db['http://www.testURI.com/loan-application-review-output']
        self.assertEqual(output['ID'], 'http://www.testURI.com/loan-application-review-output')
        self.assertEqual(output['is_a'], 'cids.Output')
        self.assertEqual(output['cids.hasName'], 'Loan Applcation Review Output')


    def test_load_organization_uris(self):
        from src.map_data import load_uris
        _ = load_uris(input_path=self.input_path)
        from src.generators.organization import generate_organization
        _ = generate_organization(input_path=self.input_path)
        org = utils.global_db['http://www.testURI.com/Organization/TestOrganization']
        self.assertEqual(org['is_a'], 'cids.Organization')
        self.assertEqual(org['ID'], 'http://www.testURI.com/Organization/TestOrganization')
        self.assertEqual(org['org.hasLegalName'], 'Test Organization Name')
        self.assertEqual(org['cids.hasImpactModel'], ['http://www.testURI.com/main-impactmodel'])
        self.assertEqual(org['cids.hasOutcome'], ['http://www.testURI.com/Outcome/Outcome1', 'http://www.testURI.com/Outcome/Outcome2'])

    def test_load_service_uris(self):
        from src.map_data import load_uris
        _ = load_uris(input_path=self.input_path)
        from src.generators.organization import generate_organization
        _ = generate_organization(input_path=self.input_path)
        service = utils.global_db['http://www.testURI.com/loan-service']
        self.assertEqual(service['ID'], 'http://www.testURI.com/loan-service')
        self.assertEqual(service['is_a'], 'cids.Service')
        self.assertEqual(service['cids.hasName'], 'Loans Service')
        self.assertEqual(service['act.hasSubActivity'], ['http://www.testURI.com/Activity/Activity1'])

    def test_load_activity_uris(self):
        from src.map_data import load_uris
        _ = load_uris(input_path=self.input_path)
        from src.generators.organization import generate_organization
        _ = generate_organization(input_path=self.input_path)
        activity = utils.global_db['http://www.testURI.com/Activity/Activity1']
        self.assertEqual(activity['ID'], 'http://www.testURI.com/Activity/Activity1')
        self.assertEqual(activity['is_a'], 'cids.Activity')
        self.assertEqual(activity['cids.hasName'], 'Activity One')
        self.assertEqual(activity['act.subActivityOf'], ['http://www.testURI.com/loan-service'])
        self.assertEqual(activity['cids.hasOutput'], ['http://www.testURI.com/Output/Output1'])

    def test_load_outcome_uris(self):
        from src.map_data import load_uris
        _ = load_uris(input_path=self.input_path)
        from src.generators.organization import generate_organization
        _ = generate_organization(input_path=self.input_path)
        output = utils.global_db['http://www.testURI.com/Output/Output1']
        self.assertEqual(output['ID'], 'http://www.testURI.com/Output/Output1')
        self.assertEqual(output['is_a'], 'cids.Output')
        self.assertEqual(output['cids.hasName'], 'Output One')

    def test_load_organization_uri_and_indicators(self):
        from src.generators.organization import generate_organization
        from src.map_data import load_indicators
        from src.map_data import load_uris
        _ = load_uris(input_path=self.input_path)
        org = generate_organization(input_path=self.input_path)
        _ = load_indicators(input_path=self.input_path)
 
        self.assertEqual(org['is_a'], 'cids.Organization')
        self.assertEqual(org['ID'], 'http://www.testURI.com/Organization/TestOrganization')
        self.assertEqual(org['cids.hasIndicator'], ['http://www.testURI.com/Indicator/Indicator2',
                                                   'http://www.testURI.com/Indicator/Indicator3','http://www.testURI.com/Indicator/Indicator4',
                                                   'http://www.testURI.com/Indicator/Indicator43','http://www.testURI.com/Indicator/Indicator44',
                                                   'http://www.testURI.com/Indicator/Indicator45','http://www.testURI.com/Indicator/Indicator46',
                                                   'http://www.testURI.com/Indicator/OutcomeIndicator1','http://www.testURI.com/Indicator/OutcomeIndicator2',
                                                   'http://www.testURI.com/Indicator/OutcomeIndicator3','http://www.testURI.com/Indicator/OutcomeIndicator4'])

    def test_load_organization_uri_and_outcomes(self):
        from src.generators.organization import generate_organization
        from src.map_data import load_indicators
        from src.map_data import load_uris
        _ = load_uris(input_path=self.input_path)
        org = generate_organization(input_path=self.input_path)
        _ = load_indicators(input_path=self.input_path)

        self.assertEqual(org['is_a'], 'cids.Organization')
        self.assertEqual(org['ID'], 'http://www.testURI.com/Organization/TestOrganization')
        self.assertEqual(org['cids.hasOutcome'], ['http://www.testURI.com/Outcome/Outcome1', 'http://www.testURI.com/Outcome/Outcome2',
                                                    'http://www.testURI.com/Outcome/Outcome3', 'http://www.testURI.com/Outcome/Outcome5',
                                                    'http://www.testURI.com/Outcome/Outcome7', 'http://www.testURI.com/Outcome/Outcome4'])

if __name__ == '__main__':
    unittest.main(exit=False)
