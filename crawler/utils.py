import json
from pathlib import Path

import numpy as np
import pandas as pd

DATA_DIR = Path(__file__).parents[1] / 'data'
VERSION_KEYS = ['current', 'previous']
CATEGORY_KEYS = ['overview', 'player', 'complete', 'league', 'league_overview']

CONSTANTS_DIR = Path(__file__).parents[1] / 'data/resources/constants/'
CURRENT_PATH = CONSTANTS_DIR / 'current.json'
PREVIOUS_PATH = CONSTANTS_DIR / 'previous.json'


def headline_attribute_from_line(line):
    equals_sign_loc = line.find('=')
    attribute_name = line[equals_sign_loc - 4: equals_sign_loc - 1]
    attribute_value = int(line[equals_sign_loc + 2:equals_sign_loc + 4])
    return {'name': attribute_name, 'value': attribute_value}


def parse_headline_attributes(headline_attributes_selector):
    attribute_dict = {}
    # note xpath is 1-indexed
    headline_attribute_script = headline_attributes_selector.xpath('./head/script')
    for line in headline_attribute_script.extract_first().split('\r\n'):
        if 'point' in line:
            attr_subdict = headline_attribute_from_line(line)
            attribute_dict[attr_subdict['name']] = attr_subdict['value']
    return attribute_dict


def read_constants():
    with open(CURRENT_PATH, 'r') as f:
        constants = json.load(f)
    return constants


def convert_currency(curr_col):
    without_euro_symbol = curr_col.str[1:]
    unit_symbol = without_euro_symbol.str[-1]
    numeric_part = np.where(unit_symbol == '0', 0, without_euro_symbol.str[:-1].pipe(pd.to_numeric))
    multipliers = unit_symbol.replace({'M': 1e6, 'K': 1e3}).pipe(pd.to_numeric)
    return numeric_part * multipliers


def filepath_tree(data_subdir_name, extension):
    sub_dir = DATA_DIR / data_subdir_name
    version_dirs = {key: sub_dir / key for key in VERSION_KEYS}
    filepaths = {ver_key:
                     {cat_key:
                          (version_dirs[ver_key] / cat_key).with_suffix(extension)
                      for cat_key in CATEGORY_KEYS}
                 for ver_key in VERSION_KEYS}
    return filepaths


def standardise_col_names(df):
    return (df
            .rename(columns=lambda col: col.lower().replace(' ', '_').replace('-', '_'))
            .rename(columns={'id': 'ID'}))
