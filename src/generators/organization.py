import pandas as pd
from ..utils.utils import get_instance, search_instances, resolve_nm

"""
File for reading input files/rows and generating Organization instances
"""

def generate_organization(row=None, klass='cids.Organization', input_path='./data/input.xlsx') -> dict:
    xls = pd.ExcelFile(input_path)
    data = pd.read_excel(xls,"Additional URIs - formatted", header=None)
    base_uri = data[data[0]=='Base URI'].iloc[0][1]
    base_uri = resolve_nm(base_uri)

    base_name = data[data[0]=='Name'].iloc[0][1]
    base_id = data[data[0]=='ID'].iloc[0][1]

    if row is None:
        organization = search_instances(klass='cids.Organization', how='first')
        if organization is None:
            organization = generate_organization(row={
                'ID'                : f'{base_uri}/{base_id}',
                'org.hasLegalName'  : base_name,
            })
    else:
        organization = generate_organization(row=row)

    im = get_instance(
        klass = 'cids.LogicModel',
        props = {
            'ID'            : f'{base_uri}/main-impactmodel',
            'org.hasName'   : 'Impact Model',
        }
    )
    organization['cids.hasImpactModel'].append(im['ID'])

    program = get_instance(
        klass = 'cids.Program',
        props = {
            'ID'            : f'{base_uri}/loan-program',
            'org.hasName'   : 'Loans Program'
        }
    )
    im['cids.hasProgram'].append(program['ID'])

    service = get_instance(
        klass = 'cids.Service',
        props = {
            'ID'            : f'{base_uri}/loan-service',
            'org.hasName'   : 'Loans Service'
        }
    )
    program['cids.hasService'].append(service['ID'])


    activity = search_instances(klass='cids.Activity', how='first')
    if activity is None:
        activity = get_instance(
            klass = 'cids.Actvity',
            props = {
                'ID'            : f'{base_uri}/loan-application-review',
                'org.hasName'   : 'Loans Application review',
            }
        )
    activity['act.subActivityOf'].append(service['ID'])
    service['act.hasSubActivity'].append(activity['ID'])

    output = search_instances(klass='cids.Output', how='first')
    if output is None:
        output = get_instance(
            klass = 'cids.Service',
            props = {
                'ID'            : f'{base_uri}/loan-application-review-output',
                'org.hasName'   : 'Loan Applcation Review Output'
            }
        )
    activity['cids.hasOutput'].append(output['ID'])

    return organization