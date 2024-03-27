import pandas as pd
from ..utils.utils import get_instance
# from .utils.utils import get_instance
"""
File for reading input files/rows and generating Indicator instances
"""

def generate_indicator(row:{}, klass=None) -> dict:
    """
    Generates a dict object that will store an instance of an Indicator
    :param row: data for indcator values.
    :type data: pd.Series, dict
    :return: dict represetnation of the newly created object
    """
    if not row.any():
        row = {}

    if klass is None:
        klass = 'cids.Indicator'

    inst = get_instance(klass=klass, props=row)

