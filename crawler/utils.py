import json
from pathlib import Path

def headline_attribute_from_line(line):
    equals_sign_loc = line.find('=')
    attribute_name = line[equals_sign_loc - 4: equals_sign_loc - 1]
    attribute_value = int(line[equals_sign_loc+2:equals_sign_loc+4])
    return {'name':attribute_name, 'value':attribute_value}

def parse_headline_attributes(soup):
    attribute_dict = {}
    headline_attribute_script = soup.find_all('script', recursive=False)[1]
    for line in headline_attribute_script.text.split('\r\n'):
        if 'point' in line:
            attr_subdict = headline_attribute_from_line(line)
            attribute_dict[attr_subdict['name']] = attr_subdict['value']
    return attribute_dict

def read_constants():
    path = Path(__file__).parents[1] / 'data/resources/constants/current.json'
    with open(path, 'r') as f:
        constants = json.load(f)
    return constants