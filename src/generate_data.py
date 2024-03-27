import pandas as pd
from .generators.organization import generate_organization
from .generators.organization import generate_organization
from .generators.indicator import generate_indicator
from .generators.indicator_report import generate_indicator_report
from .generators.output import generate_output
from .utils.utils import get_instance, search_instances

def generate_data(data) -> None:
    """
    Using loaded files, generate dict structure that matches CIDS definitions
    :param data: dictionary of values to generate instances from.
    :type data: dict
    :return: None
    """
    organization = generate_organization()
    im = get_instance(inst_id = organization['cids.hasImpactModel'])
    outcomes = search_instances(klass = 'cids.Outcome')
    im['cids.hasOutcome'] = [outcome['ID'] for outcome in outcomes]

    if data.get('indicators') is not None:
        for _,row in data.get('indicators').iterrows():
            generate_indicator(row=row)

    if data.get('outputs') is not None:
        for _,row in data.get('outputs').iterrows():
            generate_output(row=row)

    if data.get('indicators') is not None:
        for _,row in data.get('indicators').iterrows():
            generate_indicator(row=row)

    if data.get('indicator_reports') is not None:
        for _,row in data.get('indicator_reports').iterrows():
            generate_indicator_report(row=row)
