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

from src.utils import utils

class TestExcelIndicators(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.input_path=__main__.INPUT_FILE
        cls.xls = pd.ExcelFile(cls.input_path)
  
    @classmethod
    def setUp(cls):
        utils.global_db = {}


    def test_load_indicators(self):
        from src.map_data import load_indicators
        from src.map_data import load_uris
        classes2 = load_uris(input_path=self.input_path)
        classes = load_indicators(input_path=self.input_path)
        indicator_classes = [c for c in classes if c['is_a'] == 'cids.Indicator']
        self.assertEqual(len(indicator_classes), 11)

    def test_load_indicators_2(self):
        from src.map_data import load_indicators
        from src.map_data import load_uris
        classes2 = load_uris(input_path=self.input_path)
        classes = load_indicators(input_path=self.input_path)
        indicator_classes = [c for c in classes if c['is_a'] == 'cids.Indicator']

        self.assertEqual(indicator_classes[0]['ID'], 'http://www.testURI.com/Indicator/Indicator2')
        self.assertEqual(indicator_classes[0]['cids.hasName'], 'Indicator2')
        self.assertIsNone(indicator_classes[0].get('cids.hasBaseline'))
        self.assertIsNone(indicator_classes[0].get('cids.usesOutput'))
        self.assertIsNone(indicator_classes[0].get('cids.hasIndicatorReport'))
 
    def test_load_indicators_3(self):
        from src.map_data import load_indicators
        from src.map_data import load_uris
        classes2 = load_uris(input_path=self.input_path)
        classes = load_indicators(input_path=self.input_path)
        indicator_classes = [c for c in classes if c['is_a'] == 'cids.Indicator']
        self.assertEqual(indicator_classes[1]['ID'], 'http://www.testURI.com/Indicator/Indicator3')
        self.assertIn('http://www.testURI.com/IndicatorReport/Indicator3_Base', indicator_classes[1]['cids.hasIndicatorReport'])
        self.assertIn('http://www.testURI.com/IndicatorReport/Indicator3_2024_Q1', indicator_classes[1]['cids.hasIndicatorReport'])
        self.assertIn('http://www.testURI.com/IndicatorReport/Indicator3_2024_Q2', indicator_classes[1]['cids.hasIndicatorReport'])
        self.assertIn('http://www.testURI.com/IndicatorReport/Indicator3_2024_Q3', indicator_classes[1]['cids.hasIndicatorReport'])
        self.assertIn('http://www.testURI.com/IndicatorReport/Indicator3_2024_Q4', indicator_classes[1]['cids.hasIndicatorReport'])
        self.assertIn('http://www.testURI.com/IndicatorReport/Indicator3_2024_Total', indicator_classes[1]['cids.hasIndicatorReport'])
        self.assertEqual(indicator_classes[1]['cids.hasBaseline']['i72.hasUnit'], 'Number')
        self.assertEqual(indicator_classes[1]['cids.hasBaseline']['i72.numerical_value'], 55)
        self.assertEqual(indicator_classes[1]['cids.hasBaseline']['is_a'], 'i72.Measure')
        self.assertEqual(indicator_classes[1]['cids.usesOutput'], ['http://www.testURI.com/Output/Output1'])

        base = utils.global_db['http://www.testURI.com/IndicatorReport/Indicator3_Base']
        self.assertEqual(base['ID'], 'http://www.testURI.com/IndicatorReport/Indicator3_Base')
        self.assertEqual(base['is_a'], 'cids.IndicatorReport')
        self.assertEqual(base['cids.hasName'], 'Indicator3 for Base')
        self.assertEqual(base['i72.value']['i72.hasUnit'], 'Number')
        self.assertEqual(base['i72.value']['i72.numerical_value'], 55)
        self.assertEqual(base['i72.value']['is_a'], 'i72.Measure')
        
        indicator_q1 = utils.global_db['http://www.testURI.com/IndicatorReport/Indicator3_2024_Q1']
        self.assertEqual(indicator_q1['ID'], 'http://www.testURI.com/IndicatorReport/Indicator3_2024_Q1')
        self.assertEqual(indicator_q1['is_a'], 'cids.IndicatorReport')
        self.assertEqual(indicator_q1['cids.hasName'], 'Indicator3 for 2024_Q1')
        self.assertEqual(indicator_q1['i72.value']['i72.hasUnit'], 'Number')
        self.assertEqual(indicator_q1['i72.value']['i72.numerical_value'], 12)
        self.assertEqual(indicator_q1['i72.value']['is_a'], 'i72.Measure')

        indicator_q2 = utils.global_db['http://www.testURI.com/IndicatorReport/Indicator3_2024_Q2']
        self.assertEqual(indicator_q2['ID'], 'http://www.testURI.com/IndicatorReport/Indicator3_2024_Q2')
        self.assertEqual(indicator_q2['is_a'], 'cids.IndicatorReport')
        self.assertEqual(indicator_q2['cids.hasName'], 'Indicator3 for 2024_Q2')
        self.assertEqual(indicator_q2['i72.value']['i72.hasUnit'], 'Number')
        self.assertEqual(indicator_q2['i72.value']['i72.numerical_value'], 13.5)
        self.assertEqual(indicator_q2['i72.value']['is_a'], 'i72.Measure')

        indicator_q3 = utils.global_db['http://www.testURI.com/IndicatorReport/Indicator3_2024_Q3']
        self.assertEqual(indicator_q3['ID'], 'http://www.testURI.com/IndicatorReport/Indicator3_2024_Q3')
        self.assertEqual(indicator_q3['is_a'], 'cids.IndicatorReport')
        self.assertEqual(indicator_q3['cids.hasName'], 'Indicator3 for 2024_Q3')
        self.assertEqual(indicator_q3['i72.value']['i72.hasUnit'], 'Number')
        self.assertEqual(indicator_q3['i72.value']['i72.numerical_value'], 17)
        self.assertEqual(indicator_q3['i72.value']['is_a'], 'i72.Measure')

        indicator_q4 = utils.global_db['http://www.testURI.com/IndicatorReport/Indicator3_2024_Q4']
        self.assertEqual(indicator_q4['ID'], 'http://www.testURI.com/IndicatorReport/Indicator3_2024_Q4')
        self.assertEqual(indicator_q4['is_a'], 'cids.IndicatorReport')
        self.assertEqual(indicator_q4['cids.hasName'], 'Indicator3 for 2024_Q4')
        self.assertEqual(indicator_q4['i72.value']['i72.hasUnit'], 'Number')
        self.assertEqual(indicator_q4['i72.value']['i72.numerical_value'], 11)
        self.assertEqual(indicator_q4['i72.value']['is_a'], 'i72.Measure')


    def test_load_indicators_4(self):
        from src.map_data import load_indicators
        from src.map_data import load_uris
        classes2 = load_uris(input_path=self.input_path)
        classes = load_indicators(input_path=self.input_path)
        indicator_classes = [c for c in classes if c['is_a'] == 'cids.Indicator']

        self.assertEqual(indicator_classes[2]['ID'], 'http://www.testURI.com/Indicator/Indicator4')
        self.assertIn('http://www.testURI.com/IndicatorReport/Indicator4_Base', indicator_classes[2]['cids.hasIndicatorReport'])
        self.assertIn('http://www.testURI.com/IndicatorReport/Indicator4_2024_Q1', indicator_classes[2]['cids.hasIndicatorReport'])
        self.assertIn('http://www.testURI.com/IndicatorReport/Indicator4_2024_Q2', indicator_classes[2]['cids.hasIndicatorReport'])
        self.assertIn('http://www.testURI.com/IndicatorReport/Indicator4_2024_Q3', indicator_classes[2]['cids.hasIndicatorReport'])
        self.assertIn('http://www.testURI.com/IndicatorReport/Indicator4_2024_Q4', indicator_classes[2]['cids.hasIndicatorReport'])
        self.assertIn('http://www.testURI.com/IndicatorReport/Indicator4_2024_Total', indicator_classes[2]['cids.hasIndicatorReport'])
        self.assertEqual(indicator_classes[2]['cids.hasBaseline']['i72.hasUnit'], 'Percentage')
        self.assertEqual(indicator_classes[2]['cids.hasBaseline']['i72.numerical_value'], 'apple')
        self.assertEqual(indicator_classes[2]['cids.usesOutput'], ['http://www.testURI.com/Output/Output1'])

        base = utils.global_db['http://www.testURI.com/IndicatorReport/Indicator4_Base']
        self.assertEqual(base['ID'], 'http://www.testURI.com/IndicatorReport/Indicator4_Base')
        self.assertEqual(base['is_a'], 'cids.IndicatorReport')
        self.assertEqual(base['cids.hasName'], 'Indicator4 for Base')
        self.assertEqual(base['i72.value']['i72.hasUnit'], 'Percentage')
        self.assertEqual(base['i72.value']['i72.numerical_value'], 'apple')
        self.assertEqual(base['i72.value']['is_a'], 'i72.Measure')
        
        indicator_q1 = utils.global_db['http://www.testURI.com/IndicatorReport/Indicator4_2024_Q1']
        self.assertEqual(indicator_q1['ID'], 'http://www.testURI.com/IndicatorReport/Indicator4_2024_Q1')
        self.assertEqual(indicator_q1['is_a'], 'cids.IndicatorReport')
        self.assertEqual(indicator_q1['cids.hasName'], 'Indicator4 for 2024_Q1')
        self.assertEqual(indicator_q1['i72.value']['i72.hasUnit'], 'Percentage')
        self.assertEqual(indicator_q1['i72.value']['i72.numerical_value'], 'banana')
        self.assertEqual(indicator_q1['i72.value']['is_a'], 'i72.Measure')

        indicator_q2 = utils.global_db['http://www.testURI.com/IndicatorReport/Indicator4_2024_Q2']
        self.assertEqual(indicator_q2['ID'], 'http://www.testURI.com/IndicatorReport/Indicator4_2024_Q2')
        self.assertEqual(indicator_q2['is_a'], 'cids.IndicatorReport')
        self.assertEqual(indicator_q2['cids.hasName'], 'Indicator4 for 2024_Q2')
        self.assertEqual(indicator_q2['i72.value']['i72.hasUnit'], 'Percentage')
        self.assertEqual(indicator_q2['i72.value']['i72.numerical_value'], 'lemon')
        self.assertEqual(indicator_q2['i72.value']['is_a'], 'i72.Measure')

        indicator_q3 = utils.global_db['http://www.testURI.com/IndicatorReport/Indicator4_2024_Q3']
        self.assertEqual(indicator_q3['ID'], 'http://www.testURI.com/IndicatorReport/Indicator4_2024_Q3')
        self.assertEqual(indicator_q3['is_a'], 'cids.IndicatorReport')
        self.assertEqual(indicator_q3['cids.hasName'], 'Indicator4 for 2024_Q3')
        self.assertEqual(indicator_q3['i72.value']['i72.hasUnit'], 'Percentage')
        self.assertEqual(indicator_q3['i72.value']['i72.numerical_value'], 'watermelon')
        self.assertEqual(indicator_q3['i72.value']['is_a'], 'i72.Measure')

        indicator_q4 = utils.global_db['http://www.testURI.com/IndicatorReport/Indicator4_2024_Q4']
        self.assertEqual(indicator_q4['ID'], 'http://www.testURI.com/IndicatorReport/Indicator4_2024_Q4')
        self.assertEqual(indicator_q4['is_a'], 'cids.IndicatorReport')
        self.assertEqual(indicator_q4['cids.hasName'], 'Indicator4 for 2024_Q4')
        self.assertEqual(indicator_q4['i72.value']['i72.hasUnit'], 'Percentage')
        self.assertEqual(indicator_q4['i72.value']['i72.numerical_value'], 'raspberry')
        self.assertEqual(indicator_q4['i72.value']['is_a'], 'i72.Measure')



    def test_load_indicators_43(self):
        from src.map_data import load_indicators
        from src.map_data import load_uris
        classes2 = load_uris(input_path=self.input_path)
        classes = load_indicators(input_path=self.input_path)
        indicator_classes = [c for c in classes if c['is_a'] == 'cids.Indicator']

        self.assertEqual(indicator_classes[3]['ID'], 'http://www.testURI.com/Indicator/Indicator43')
        self.assertEqual(indicator_classes[3]['cids.hasName'], 'Indicator43')
        self.assertIsNone(indicator_classes[3].get('cids.hasBaseline'))
        self.assertIsNone(indicator_classes[3].get('cids.usesOutput'))
        self.assertIsNone(indicator_classes[3].get('cids.hasIndicatorReport'))

    def test_load_indicators_44(self):
        from src.map_data import load_indicators
        from src.map_data import load_uris
        classes2 = load_uris(input_path=self.input_path)
        classes = load_indicators(input_path=self.input_path)
        indicator_classes = [c for c in classes if c['is_a'] == 'cids.Indicator']

        self.assertEqual(indicator_classes[4]['ID'], 'http://www.testURI.com/Indicator/Indicator44')
        self.assertEqual(indicator_classes[4]['cids.hasName'], 'Indicator44')
        self.assertIsNone(indicator_classes[4].get('cids.hasBaseline'))
        self.assertIsNone(indicator_classes[4].get('cids.usesOutput'))
        self.assertIsNone(indicator_classes[4].get('cids.hasIndicatorReport'))

    def test_load_indicators_45(self):
        from src.map_data import load_indicators
        from src.map_data import load_uris
        classes2 = load_uris(input_path=self.input_path)
        classes = load_indicators(input_path=self.input_path)
        indicator_classes = [c for c in classes if c['is_a'] == 'cids.Indicator']

        self.assertEqual(indicator_classes[5]['ID'], 'http://www.testURI.com/Indicator/Indicator45')
        self.assertEqual(indicator_classes[5]['cids.hasName'], 'Indicator45')
        self.assertIsNone(indicator_classes[5].get('cids.hasBaseline'))
        self.assertIsNone(indicator_classes[5].get('cids.usesOutput'))
        self.assertIsNone(indicator_classes[5].get('cids.hasIndicatorReport'))

    def test_load_indicators_46(self):
        # global utils.global_db 
        from src.map_data import load_indicators
        from src.map_data import load_uris
        classes2 = load_uris(input_path=self.input_path)
        classes = load_indicators(input_path=self.input_path)
        indicator_classes = [c for c in classes if c['is_a'] == 'cids.Indicator']

        self.assertEqual(indicator_classes[6]['ID'], 'http://www.testURI.com/Indicator/Indicator46')
        self.assertEqual(indicator_classes[6]['cids.hasName'], 'Indicator46')
        self.assertIsNone(indicator_classes[6].get('cids.hasBaseline'))
        self.assertIsNone(indicator_classes[6].get('cids.usesOutput'))
        self.assertIsNone(indicator_classes[6].get('cids.hasIndicatorReport'))


    def test_load_outcome_indicators_1(self):
        # global utils.global_db 
        from src.map_data import load_indicators
        from src.map_data import load_uris
        classes2 = load_uris(input_path=self.input_path)
        classes = load_indicators(input_path=self.input_path)
        indicator_classes = [c for c in classes if c['is_a'] == 'cids.Indicator']

        self.assertEqual(indicator_classes[7]['ID'], 'http://www.testURI.com/Indicator/OutcomeIndicator1')
        utils.global_db['http://www.testURI.com/Indicator/OutcomeIndicator1']
        self.assertEqual(indicator_classes[7]['cids.hasName'], 'Outcome1 rating score total')

    def test_load_outcome_indicators_2(self):
        # global utils.global_db 
        from src.map_data import load_indicators
        from src.map_data import load_uris
        classes2 = load_uris(input_path=self.input_path)
        classes = load_indicators(input_path=self.input_path)
        indicator_classes = [c for c in classes if c['is_a'] == 'cids.Indicator']

        self.assertEqual(indicator_classes[8]['ID'], 'http://www.testURI.com/Indicator/OutcomeIndicator2')
        self.assertEqual(indicator_classes[8]['cids.hasName'], 'Outcome2 rating score total')

    def test_load_outcome_indicators_3(self):
        # global utils.global_db 
        from src.map_data import load_indicators
        from src.map_data import load_uris
        classes2 = load_uris(input_path=self.input_path)
        classes = load_indicators(input_path=self.input_path)
        indicator_classes = [c for c in classes if c['is_a'] == 'cids.Indicator']

        self.assertEqual(indicator_classes[9]['ID'], 'http://www.testURI.com/Indicator/OutcomeIndicator3')
        self.assertEqual(indicator_classes[9]['cids.hasName'], 'Outcome3 rating score total')

    def test_load_outcome_indicators_4(self):
        # global utils.global_db 
        from src.map_data import load_indicators
        from src.map_data import load_uris
        classes2 = load_uris(input_path=self.input_path)
        classes = load_indicators(input_path=self.input_path)
        indicator_classes = [c for c in classes if c['is_a'] == 'cids.Indicator']

        self.assertEqual(indicator_classes[10]['ID'], 'http://www.testURI.com/Indicator/OutcomeIndicator4')
        self.assertEqual(indicator_classes[10]['cids.hasName'], 'Outcome4 rating score total')

    def test_load_indicator_report_1(self):
        from src.map_data import load_indicators
        from src.map_data import load_uris

        classes2 = load_uris(input_path=self.input_path)
        classes = load_indicators(input_path=self.input_path)

        report = utils.global_db['http://www.testURI.com/ImpactReport/Indicator4_2024_Targets']
        self.assertEqual(report['ID'], 'http://www.testURI.com/ImpactReport/Indicator4_2024_Targets')
        self.assertEqual(report['is_a'], 'cids.ImpactReport')
        self.assertEqual(report['cids.hasName'], 'Indicator4 for 2024 Targets')
        self.assertEqual(report['cids.hasExpectation']['is_a'], 'i72.Measure')
        self.assertEqual(report['cids.hasExpectation']['i72.hasUnit'], 'Percentage')
        self.assertEqual(report['cids.hasExpectation']['i72.numerical_value'], 'orange')

if __name__ == '__main__':
    unittest.main(exit=False)
