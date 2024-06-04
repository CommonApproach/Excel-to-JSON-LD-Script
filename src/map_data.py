from collections import defaultdict
import pandas as pd
import warnings


from .utils.utils import get_instance, search_instances, PropertyList, ObjectDict, resolve_nm

"""
The <mappings> dictionary stores mappgins between Class properties in the Excel file and those in CIDS.
"""
mappings = {
    'Measure'   : {
        'Unit_of_measure'   : 'i72.hasUnit',
        'Base'              : 'cids.hasBaseline',
    },
    'Indicator' : {
        'ID'                : 'ID',
        'Name'              : 'cids.hasName',
        'Description'       : 'cids.hasDescription',
        'Unit_of_measure'   : 'i72.hasUnit',
        'Base'              : 'cids.hasBaseline',
        'usesOutput'        : 'cids.usesOutput',
    },
    'Theme'     : {
        'ID'                : 'ID',
        'Name'              : 'cids.hasName',
        'Description'       : 'cids.hasDescription',
    },
    'Outcome'   : {
        'ID'                : 'ID',
        'Name'              : 'cids.hasName',
        'Description'       : 'cids.hasDescription',
        'ForTheme'          : 'cids.forTheme',
    },
    'Organization'    : {
        'ID'                : 'ID',
        'Name'              : 'org.hasLegalName',
    },
    'Activity'      : {
        'ID'                : 'ID',
        'Name'              : 'cids.hasName',
        'Description'       : 'cids.hasDescription',
        'hasOutput'         : 'cids.hasOutput',
    },
    'Output'    : {
        'ID'                : 'ID',
        'Name'              : 'cids.hasName',
        'Description'       : 'cids.hasDescription',
        'UsedbyIndicator'   : 'cids.usedByIndicator',
    }
}
class_list_properties = {
    'Output'        : ['UsedbyIndicator']
}

def load_indicators(input_path='./data/input.xlsx'):
    """
    Creates Indicators and related classes from Excel file.
    :param input_path: path to inptu Excel sheet.
    :type input_path: str
    :returns: list: List of Indicator instances created.
    """
    organization = search_instances(klass='cids.Organization', how='first')
    if organization is None:
        raise("No organization found")
    xls = pd.ExcelFile(input_path)
    sheet_name = 'Indicators - formatted'

    df = pd.read_excel(xls,sheet_name, header=1)
    tmp_data = pd.read_excel(xls,"Additional URIs - formatted", header=None)
    base_uri = tmp_data[tmp_data[0]=='Base URI'].iloc[0][1]
    base_uri = resolve_nm(base_uri)

    # we have one service to which all outcomes and outputs will be added.
    indicator_output = search_instances(klass='cids.Output', how='first')

    # Row indexes
    # Indicator starting row
    indicator_row_i = 2

    # Column indexes
    range_row = df.iloc[0]
    # get column where Indicators and values start
    indicator_col_i = range_row.index.tolist().index('Indicator Names')
    # get column where Outcomes for each indicator start
    outcome_indicator_col_i = range_row.index.tolist().index('forOutcome')
    indicator_report_col_i = range_row.index.tolist().index('IndicatorURI') + 1
    indicator_value_cols = [col for col in df.columns[indicator_col_i:indicator_report_col_i] if not col.endswith("_Targets")]
    indicator_target_cols = [col for col in df.columns[indicator_col_i:indicator_report_col_i] if col.endswith("_Targets")]
    
    # theme = None
    indicators = []
    for _, row in df.iloc[indicator_row_i:].iterrows():
        if not pd.isnull(row['IndicatorURI']):
            # create instance of Indicator
            indicator_id = resolve_nm(row['IndicatorURI'])
            indicator_name = row['Indicator Names']

            indicator = get_instance(klass='cids.Indicator', props={'ID':indicator_id, 'cids.hasName':indicator_name})

            # create base Measure for teh indicator, if value exists
            if not pd.isnull(row.get('Base')):
                base_measure = get_instance(klass='i72.Measure')
                base_measure['i72.numerical_value'] = row.get('Base')
                if not pd.isnull(row.get('Unit_of_measure')): base_measure[mappings['Indicator']['Unit_of_measure']] = row['Unit_of_measure']
                indicator[mappings['Indicator']['Base']] = base_measure

                if indicator_output is not None:
                    indicator[mappings['Indicator']['usesOutput']].append(indicator_output['ID'])
                else:
                    warnings.warn(f"No Indicator Output provided")


            # For each Indicator value, create a corresponding Indicator Report instance
            for col in indicator_value_cols:
                if col+'.1' in row.index and not pd.isnull(row[col]):
                    indicator_report_id = resolve_nm(row[col+'.1'])
                    indicator_report_name = f"{indicator['cids.hasName']} for {col}"
                    indicator_report = get_instance(klass='cids.IndicatorReport', props={'ID':indicator_report_id, 'cids.hasName':indicator_report_name})
                    measure = get_instance(klass='i72.Measure')
                    if not pd.isnull(row.get('Unit_of_measure')): measure[mappings['Indicator']['Unit_of_measure']] = row['Unit_of_measure']
                    measure['i72.numerical_value'] = row[col]
                    indicator_report['i72.value'] = measure
                    indicator['cids.hasIndicatorReport'].append(indicator_report['ID'])

            # For each Indicator target value, create a corresponding Impact Report instance
            impact_reports = []
            for col in indicator_target_cols:
                if col+'.1' in row.index and not pd.isnull(row[col]):
                    measure = get_instance(klass='i72.Measure')
                    if not pd.isnull(row.get('Unit_of_measure')): measure[mappings['Indicator']['Unit_of_measure']] = row['Unit_of_measure']
                    measure['i72.numerical_value'] = row[col]
                    impact_report_id = resolve_nm(f"{base_uri}/ImpactReport/{row['ShortCode']}_{col}")
                    impact_report_name = f"{row['ShortCode']} for {col}".replace('_', ' ')
                    impact_report = get_instance(klass='cids.ImpactReport', props={'ID':impact_report_id, 'cids.hasName': impact_report_name})
                    impact_report['cids.hasExpectation'] = measure
                    impact_reports.append(impact_report)

            # For each OutcomreIndicator, associate an Outcome with the Indicator.
            for outcome_uri in row[row.index[outcome_indicator_col_i:]].dropna().values.tolist():
                outcome_uri = resolve_nm(outcome_uri)
                outcome = search_instances(klass='cids.Outcome', props={'ID':outcome_uri}, how='first')
                if outcome is None:
                    outcome = get_instance(klass='cids.Outcome', props={'ID':outcome_uri})

                outcome['cids.hasIndicator'].append(indicator['ID'])
                organization['cids.hasOutcome'].append(outcome['ID'])

                for impact_report in impact_reports:
                    impact_report['cids.forOutcome'] = outcome['ID']

            indicators.append(indicator)
            organization['cids.hasIndicator'].append(indicator['ID'])

    return indicators

# To epxlictly define the range and restrictions of a class property, the <restrictions> dictionary is used.
restrictions = {
    'cids.Organization' : {
        'org.hasLegalName':'one',
        'cids.hasName':'one',
        'cids.hasDescription':'one',
    },
    'cids.Theme' : {
        'cids.hasName':'one',
        'cids.hasDescription':'one',
    },
    'cids.Outcome' : {
        'cids.hasName':'one',
        'cids.hasDescription':'one',
        'cids.forTheme':'many',
    },
    'cids.Stakeholder': {
        'cids.hasName':'one',
        'cids.hasDescription':'one',
        'org.hasRole':'one',
    },
    'cids.Characteristic' : {
        'cids.hasName':'one',
        'i72.value':'list',
        'cids.generatedBy':'many',
    },
    'cids.Activity': {
        'cids.hasName':'one',
        'cids.hasDescription':'one',
        'cids.hasOutput': 'many',
    },
    'cids.Output': {
        'cids.hasName':'one',
        'cids.hasDescription':'one',
        'cids.usedByIndicator': 'many',
    }
}
def load_uris(input_path='./data/input.xlsx'):
    """
    Creates Class instances found in the input Excel file.
    For any Organizaiton instances found, all Indicator and Outcome instances are explicitly 
    associated with the Organization instances via cids:hasIndicator and cids:hasOutcome, respectively.
    :param input_path: path to inptu Excel sheet.
    :type input_path: str
    :returns: list: List of class instances created.
    """
    xls = pd.ExcelFile(input_path)
    sheet_name = 'Additional URIs - formatted'
    df = pd.read_excel(xls,sheet_name, header=None)
    columns = None
    classes = []

    for idx,row in df.iloc[1:].iterrows():
        if row[0] == 'Class':
            columns = row.values
            continue
        elif columns is None:
            continue
        elif row.dropna().empty:
            continue

        row.index = columns
        
        if mappings.get(row.Class):
            if class_list_properties.get(row.Class):
                row_copy = row[class_list_properties.get(row.Class)].copy()
                for col, val in row_copy.items():
                    vals = PropertyList([resolve_nm(v.strip()) for v in val.split(',')])
                    row[col] = vals

            props = ObjectDict(list)
            for _from, _to in mappings[row.Class].items():
                res = restrictions.get(f'cids.{row.Class}')
                if isinstance(row[_from], (pd.Series, PropertyList, list)):
                    props[_to] = [resolve_nm(val) for val in row[_from] if val]
                    props[_to] = [p for p in props[_to] if not pd.isnull(p)]
                elif pd.isnull(row[_from]):
                    continue
                elif _to == 'ID':
                    props[_to] = resolve_nm(row[_from])
                else:
                    props[_to].append(row[_from])

                if isinstance(res, dict) and res.get(_to) is not None:
                    res_mapped = res.get(_to)
                    if res_mapped == 'one' and props[_to] != []:
                        props[_to] = props[_to][0]

            inst = get_instance(klass=f'cids.{row.Class}', props=props)
            classes.append(inst)

    org = [c for c in classes if c['is_a'] == 'cids.Organization']
    if org != []:
        org[0]['cids.hasIndicator'] += [c['ID'] for c in classes if c['is_a'] == 'cids.Indicator']
        org[0]['cids.hasOutcome'] += [c['ID'] for c in classes if c['is_a'] == 'cids.Outcome']

    return classes
