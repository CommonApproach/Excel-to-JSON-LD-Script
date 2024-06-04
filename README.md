# excel-to-json-etl

# Input Data
Input CSV/Excel files are stored in the [./data](data) folder.

# Output Data
Generated JSON files are stored in the [./output](output) folder.

# Installation
1. Create conda environment 
    ```shell
    conda env create -f environment.yml
    ```

# Configuration
In [./src/main.py](./src/main.py), you can configure the source files and properties:

- `INPUT_FILE` : local path to input Excel file, e.g. `./data/input.xlsx`.
- `OUTPUT_FILE` : local path to output file, e.g. `./output/output.json`.
- `CIDS_URL` : URL for the CIDS OWL file.
- `CONTEXT_PATH` : URL for the context file.
- `REPLACE_PREFIX` : How should context replacements be done. Options: 'context_only', 'all', 'label_only', 'none'

# Execute

1. Activate conda environment
    ```shell
    conda activate PyExcelToJSONETL
    ```

2. Run ETL Pipeline
    ```shell
    python -m src.main
    ```



# Instructions
Update the input file defined by ``INPUT_FILE``.
 - Any Indicators in the ``Indicators - formatted`` sheet are imported. Only those with values are associated with measures and reports.
 - Any instances in the ``Additional URIs - formatted`` sheet are imported. 
 - Default classes required for the Impact Model are defined in [src/generators/organization.py](src/generators/organization.py) ``generate_organization()``.
    - To change the use case from the defaalt "loan" use case, concepts in  must be updated. Program and Service instances added to the URIS will not be associated with the organization.
    - If an Organization is provided in the sheet, the default one is not used.
    - If an Outcome is provided in the URIs sheet, the default one is not used.
    - If an Activity is provided in the URIs sheet, the default one is not used.
    - If a Program instance is provided in the URIs sheet, the default one is still used.
    - If a Service instances is provided in the URIs sheet, the default one is still used.
 
 - To epxlictly define the range and restrictions of a class property, the `restrictions` dictionary in [src/utils/map_data.py](src/utils/map_data.py) is used.

 Main flow of the program is the following:
 - main.py
   1. Load URIs from spreadsheet: `load_uris(input_path=INPUT_FILE)`
   1. Load Default organization and related concepts: `generate_organization(input_path=INPUT_FILE)`
   1. Load Indicator instances:  `load_indicators(input_path=INPUT_FILE)`
   1. Export data to JSON: `export_json(out_path=OUTPUT_FILE)`


 
 
# Run Tests
Unit tests validate two inputs from the test file [tests/ExceltoJSONTemplate-test.xlsx](ExceltoJSONTemplate-test.xlsx). If the format of the main input file is changed, this file should also be updated for tests. 
 - tests 
   - [tests/test_excel_indicators.py](tests/test_excel_indicators.py) tests the concepts in the ``Indicators - formatted`` sheet.
   - [tests/test_excel_uris.py](tests/test_excel_uris.py) tests the concepts in the ``Additional URIs - formatted`` sheet.
   - [tests/test_excel_organizations.py](tests/test_excel_organizations.py) tests organizations from two sources:
     - Default organization and related concepts (src/generators.organization.py)[src/generators.organization.py] ``generate_organization()``.
     - Organization defined in the ``Additional URIs - formatted`` sheet. If it defined, those in generate_organization() are not created.
  - [tests/test_excel_context_context_only.py](tests/test_excel_context_context_only.py) tests the context file configurations. 
    - Assumes that only defintions in the context file that match namespaces prefixes are renamed. 
    - `CONTEXT_PATH` stores the base URL for the first context file.
    - The test only checks for the `org:hasLegalName` on the first Organization to ensure the context file was read in correctly.

1. Activate conda environment
    ```shell
    conda activate PyExcelToJSONETL
    ```
2. Run tests Pipeline
    ```shell
    python -m unittest
    ```
