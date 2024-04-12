import os
LOG_FILE = f"./logs/logs.txt"    # Log file used to log errors or warnings that should not stop the processing (e.g. invalid address is found)
if os.path.exists(LOG_FILE):
    os.remove(LOG_FILE)
if not os.path.exists('./output/'):
    os.makedirs('./output/')

INPUT_FILE = './data/ExceltoJSONTemplate.xlsx'
CIDS_URL = 'http://ontology.commonapproach.org/owl/cids_v2.1.owl'
CONTEXT_PATH = "https://ontology.commonapproach.org/contexts/cidsContext.json"
REPLACE_PREFIX = 'none' # options: 'context_only', 'label_only', 'all', 'none'

from .utils.contexts import load_context_mapping
MAPPING_CONTEXT, _ = load_context_mapping(path=CONTEXT_PATH)


import pandas as pd
from .utils.namespaces import namespaces, cids, PREFIX, BASE_PREFIX
exec(f"from .utils.namespaces import {','.join([nm for nm in namespaces.keys() if nm != ''])}")
# from .utils.utils import row_to_jsonld
from owlready2 import Thing, ThingClass, DataPropertyClass


from .load_ontology import import_cids
from .generate_data import generate_data
from .export_data import export_json
from .map_data import load_indicators, load_uris
from .generators.organization import generate_organization

import warnings
warnings.filterwarnings("ignore")


if __name__ == '__main__':
    with cids:
        # process CIDS ontology
        import_cids()
        

        # Extract: load_data and map to right mapping columns
        _=load_uris(input_path=INPUT_FILE)
        _ = generate_organization(input_path=INPUT_FILE)
        _=load_indicators(input_path=INPUT_FILE)

        # Load: export data to JSON
        export_json()
