import requests
import json
from bs4 import BeautifulSoup, SoupStrainer
from pathlib import Path
import shutil
from .utils import parse_headline_attributes

CONSTANTS_DIR = Path(__file__).parents[1] / 'data/resources/constants/'
CURRENT_PATH = CONSTANTS_DIR / 'current.json'
PREVIOUS_PATH = CONSTANTS_DIR / 'previous.json'


def get_all_traits_and_specialities():
    url = 'https://sofifa.com/players/top'
    html = requests.get(url).text
    strainer = SoupStrainer('form', action='/players', class_='pjax relative')
    soup = BeautifulSoup(html, 'lxml', parse_only=strainer)
    traits1 = list(soup.find(attrs={'name': 't1[]'}).stripped_strings)
    traits2 = list(soup.find(attrs={'name': 't2[]'}).stripped_strings)
    all_traits = [*traits1, *traits2]
    all_specialities = list(soup.find(attrs={'name': 'sc[]'}).stripped_strings)
    return {'traits': all_traits, 'specialities': all_specialities}

def get_headline_attribute_names():
    url = 'https://sofifa.com/player/158023'
    html = requests.get(url).text
    strainer = SoupStrainer('script')
    soup = BeautifulSoup(html, 'lxml', parse_only=strainer)
    return list(parse_headline_attributes(soup).keys())

def get_all_constants():
    positions = ['RS', 'RW', 'RF', 'RAM', 'RCM', 'RM', 'RDM', 'RCB', 'RB',
                 'RWB', 'ST', 'LW', 'CF', 'CAM', 'CM', 'LM', 'CDM', 'CB',
                 'LB', 'LWB', 'LS', 'LF', 'LAM', 'LCM', 'LDM', 'LCB']
    traits_specialities_dict = get_all_traits_and_specialities()
    headline_attribute_names = get_headline_attribute_names()
    constants_dict = {'positions':positions,
                      'headline_attributes':headline_attribute_names,
                      **traits_specialities_dict}
    return constants_dict

def save_constants():
    path = CURRENT_PATH
    constants_dict = get_all_constants()
    with open(path, 'w') as f:
        json.dump(constants_dict, f)

def update_constants():
    try:
        shutil.move(CURRENT_PATH, PREVIOUS_PATH)
    except FileNotFoundError:
        pass
    save_constants()


if __name__ == '__main__':
    update_constants()


