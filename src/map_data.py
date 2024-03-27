from collections import defaultdict
import pandas as pd

from .utils.utils import get_instance, search_instances, PropertyList, ObjectDict, resolve_nm

mappings = {
    'Measure'   : {
        'Unit_of_measure'   : 'i72.hasUnit',
        'Base'              : 'cids.hasBaseline',
    },
    'Indicator' : {
        'ID'                : 'ID',
        'Name'              : 'org.hasName',
        'Description'       : 'org.hasDescription',
        'Unit_of_measure'   : 'i72.hasUnit',
        'Base'              : 'cids.hasBaseline',
        'usesOutput'        : 'cids.usesOutput',
    },
    'Theme'     : {
        'ID'                : 'ID',
        'Name'              : 'org.hasName',
        'Description'       : 'org.hasDescription',
    },
    'Outcome'   : {
        'ID'                : 'ID',
        'Name'              : 'org.hasName',
        'Description'       : 'org.hasDescription',
        'ForTheme'          : 'cids.forTheme',
    },
    'Organization'    : {
        'ID'                : 'ID',
        'Name'              : 'org.hasLegalName',
    },
    'Activity'      : {
        'ID'                : 'ID',
        'Name'              : 'org.hasName',
        'Description'       : 'org.hasDescription',
        'hasOutput'         : 'cids.hasOutput',
    },
    'Output'    : {
        'ID'                : 'ID',
        'Name'              : 'org.hasName',
        'Description'       : 'org.hasDescription',
        'UsedbyIndicator'   : 'cids.usedByIndicator',
    }
}
class_list_properties = {
    'Output'        : ['UsedbyIndicator']
}
indicator_report_mapping = {}
organization_mapping = {}
output_mapping = {}


def load_indicators(input_path='./data/input.xlsx'):
    xls = pd.ExcelFile(input_path)
    sheet_name = 'Indicators - formatted'

    df = pd.read_excel(xls,sheet_name, header=1)
    tmp_data = pd.read_excel(xls,"Additional URIs - formatted", header=None)
    base_uri = tmp_data[tmp_data[0]=='Base URI'].iloc[0][1]
    base_uri = resolve_nm(base_uri)

    # we ave one service to which all outcomes and outputs will be added.
    indicator_output = search_instances(klass='cids.Output', how='first')

    # Row indexes
    # Indicator starting row
    indicator_row_i = 2
    # SDG Outcome Indicator starting row
    # sdg_indicator_row_i = df[df['IndicatorURI'] == 'Outcome URI'].iloc[0].name
    # sdg_indicator_row_i = []

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
    for idx, row in df.iloc[indicator_row_i:].iterrows():
        # if idx != sdg_indicator_row_i:

        if not pd.isnull(row['IndicatorURI']):
            indicator_id = resolve_nm(row['IndicatorURI'])
            indicator_name = row['Indicator Names']

            indicator = get_instance(klass='cids.Indicator', props={'ID':indicator_id, 'org.hasName':indicator_name})
            if not pd.isnull(row.get('Base')):
                base_measure = get_instance(klass='i72.Measure')
                base_measure['i72.numerical_value'] = row.get('Base')
                if not pd.isnull(row.get('Unit_of_measure')): base_measure[mappings['Indicator']['Unit_of_measure']] = row['Unit_of_measure']
                indicator[mappings['Indicator']['Base']].append(base_measure)

                indicator[mappings['Indicator']['usesOutput']].append(indicator_output['ID'])

            for col in indicator_value_cols:
                if col+'.1' in row.index and not pd.isnull(row[col]):
                    indicator_report_id = resolve_nm(row[col+'.1'])
                    indicator_report_name = f"{indicator['org.hasName']} for {col}"
                    indicator_report = get_instance(klass='cids.IndicatorReport', props={'ID':indicator_report_id, 'org.hasName':indicator_report_name})
                    measure = get_instance(klass='i72.Measure')
                    if not pd.isnull(row.get('Unit_of_measure')): measure[mappings['Indicator']['Unit_of_measure']] = row['Unit_of_measure']
                    measure['i72.numerical_value'] = row[col]
                    indicator['cids.hasIndicatorReport'].append(indicator_report['ID'])
            impact_reports = []
            for col in indicator_target_cols:
                if col+'.1' in row.index and not pd.isnull(row[col]):
                    measure = get_instance(klass='i72.Measure')
                    if not pd.isnull(row.get('Unit_of_measure')): measure[mappings['Indicator']['Unit_of_measure']] = row['Unit_of_measure']
                    measure['i72.numerical_value'] = resolve_nm(row[col])
                    impact_report_id = resolve_nm(f"{base_uri}/ImpactReport/{row['ShortCode']}_{col}")
                    impact_report_name = f"{row['ShortCode']} for {col}".replace('_', ' ')
                    impact_report = get_instance(klass='cids.ImpactReport', props={'ID':impact_report_id, 'org.hasName': impact_report_name})
                    impact_report['cids.hasExpectation'].append(measure)
                    impact_reports.append(impact_report)
            for outcome_uri in row[row.index[outcome_indicator_col_i:]].dropna().values.tolist():
                outcome_uri = resolve_nm(outcome_uri)
                outcome = search_instances(klass='cids.Outcome', props={'ID':outcome_uri}, how='first')
                if outcome is None:
                    outcome = get_instance(klass='cids.Outcome', props={'ID':outcome_uri})

                outcome['cids.hasIndicator'].append(indicator['ID'])
                for impact_report in impact_reports:
                    impact_report['cids.forOutcome'].append(outcome['ID'])

            indicators.append(indicator)


    return indicators


def load_uris(input_path='./data/input.xlsx'):
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
                if isinstance(row[_from], (pd.Series, PropertyList, list)):
                    props[_to] = [resolve_nm(val) for val in row[_from] if val]
                elif pd.isnull(row[_from]):
                    continue
                elif _to == 'ID':
                    props[_to] = resolve_nm(row[_from])
                else:
                    props[_to].append(row[_from])

            inst = get_instance(klass=f'cids.{row.Class}', props=props)
            classes.append(inst)

    return classes
