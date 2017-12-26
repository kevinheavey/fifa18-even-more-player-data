import json
import shutil
import numpy as np
import requests
import parsel

from crawler.utils import parse_headline_attributes, CURRENT_PATH, PREVIOUS_PATH


def get_all_traits_and_specialities():
    url = 'https://sofifa.com/players/top'
    html = requests.get(url).text
    selector = parsel.Selector(html)
    path = './body/section[1]/section[1]/aside[1]/form[1]/div[last()]/div[position() >= last() - 2]/select'
    relevant = selector.xpath(path)
    end_of_path = 'option/text()'
    traits_raw = relevant[:2].xpath(end_of_path).extract()
    # some traits are duplicated unfortunately
    traits = list(np.unique([t.strip() for t in traits_raw if t != 'trait.']))
    all_traits = [t + '_trait' for t in traits]
    specialities_raw = relevant[2].xpath(end_of_path).extract()
    # it's important to add the speciality flag
    # because there is a strength speciality and a strength attribute
    all_specialities = [s.strip() + '_speciality' for s in specialities_raw]
    return {'traits': all_traits, 'specialities': all_specialities}

def get_headline_attribute_names():
    url = 'https://sofifa.com/player/158023'
    html = requests.get(url).text
    # below is a hacky way to get a selector that fits the parse_headline_attributes function
    script_html = parsel.Selector(text=html).xpath('/html/body/script[1]').extract_first()
    selector = parsel.Selector(script_html)
    return list(parse_headline_attributes(selector).keys())



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
    uncategorised = ['ID', 'full_name', 'Birth date', 'Release clause']
    test_ids = [192046, 233588, 211873, 190658, 220314,
                186551, 193850, 11793, 190432, 189035]
    constants_dict = {'positions':positions,
                      'position_preferences':position_preferences,
                      'headline_attributes':headline_attribute_names,
                      'main_attributes':main_attributes,
                      'body_features':body_features,
                      'special_attributes':special_attributes,
                      'uncategorised':uncategorised,
                      'test_ids':test_ids,
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


