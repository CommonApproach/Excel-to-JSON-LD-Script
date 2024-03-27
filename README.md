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

