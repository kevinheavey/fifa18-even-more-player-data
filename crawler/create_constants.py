import requests
import json
from bs4 import BeautifulSoup, SoupStrainer
from pathlib import Path
import shutil
from crawler.utils import parse_headline_attributes
import numpy as np

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
    all_traits = list(np.unique([*traits1, *traits2])) # some traits are duplicated unfortunately
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
                 'LB', 'LWB', 'LS', 'LF', 'LAM', 'LCM', 'LDM', 'LCB', 'GK']
    main_attributes = ['Crossing','Finishing','Heading accuracy',
                       'Short passing','Volleys','Dribbling','Curve',
                       'Free kick accuracy','Long passing','Ball control',
                       'Acceleration','Sprint speed','Agility','Reactions',
                       'Balance','Shot power','Jumping','Stamina','Strength',
                       'Long shots','Aggression','Interceptions','Positioning',
                       'Vision','Penalties','Composure','Marking','Standing tackle',
                       'Sliding tackle','GK diving','GK handling','GK kicking',
                       'GK positioning','GK reflexes',]
    position_preferences = ['prefers_' + pos for pos in positions]
    traits_specialities_dict = get_all_traits_and_specialities()
    headline_attribute_names = get_headline_attribute_names()
    body_features = ['Height_cm', 'Weight_kg', 'Body type', 'Real face']
    special_attributes = ['International reputation', 'Skill moves',
                          'Weak foot', 'Work rate att', 'Work rate def',
                          'Preferred foot']
    uncategorised = ['ID', 'Birth date', 'Release clause']
    constants_dict = {'positions':positions,
                      'position_preferences':position_preferences,
                      'headline_attributes':headline_attribute_names,
                      'main_attributes':main_attributes,
                      'body_features':body_features,
                      'special_attributes':special_attributes,
                      'uncategorised':uncategorised,
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


