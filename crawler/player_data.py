import pandas as pd
import numpy as np
from bs4 import BeautifulSoup, SoupStrainer
from .utils import parse_headline_attributes, read_constants, convert_currency
from .html_download import get_player_htmls

def _get_main_soup(soup):
    return soup.find_all('section', recursive=False)[2]


def _get_main_article(main_soup):
    return main_soup.section.article


def _get_col3_divs(main_article):
    col_divs = (main_article
                .find_all('div', class_='columns', recursive=False))
    col3_divs = []
    for sub_div in col_divs:
        col3_divs.extend(sub_div.find_all('div', class_='col-3', recursive=False))
    return col3_divs


def parse_main_attributes(col3_divs):
    attribute_dict = {}
    for sub_div in col3_divs[:-1]:  # last one is traits and specialities
        for li in sub_div.div.ul.find_all('li', recursive=False):
            stripped_strings = list(li.stripped_strings)
            attribute_name = stripped_strings[-1]
            attribute_value = stripped_strings[0]
            attribute_dict[attribute_name] = attribute_value
    return attribute_dict


def parse_player_metadata(main_article):
    attribute_dict = {}
    player_info_soup = main_article.div.div.div
    stripped_strings = list(player_info_soup.span.stripped_strings)
    attribute_dict['preferred_positions'] = stripped_strings[1:-1]
    age_height_weight = stripped_strings[-1].split()
    attribute_dict['Birth date'] = ' '.join(age_height_weight[2:5]).replace(',', '').strip('(').strip(')')
    attribute_dict['Height_cm'] = age_height_weight[5].strip('cm')
    attribute_dict['Weight_kg'] = age_height_weight[-1].strip('kg')

    return attribute_dict


def _get_traits_and_specialities_dict(player_traits, player_specialities, all_traits, all_specialities):
    trait_dict = {trait: (trait in player_traits) for trait in all_traits}
    speciality_dict = {speciality: (speciality in player_specialities) for speciality in all_specialities}
    return {**trait_dict, **speciality_dict}


def parse_traits_and_specialities(col3_divs, all_traits, all_specialities):
    last_div = col3_divs[-1]
    if not last_div.text.strip():
        player_traits, player_specialities = [np.nan], [np.nan]
    else:
        uls = last_div.div.find_all('ul', recursive=False)
        n_uls = len(uls)
        if n_uls == 1:
            ul = uls[0]
            ul_strings = list(ul.stripped_strings)
            ul_h5 = ul.parent.h5.text
            if ul_h5 == 'Traits':
                player_traits = ul_strings
                player_specialities = [np.nan]
            elif ul_h5 == 'Specialities':
                player_traits = [np.nan]
                player_specialities = ul_strings
        else:
            player_traits = list(uls[0].stripped_strings)
            player_specialities = list(uls[1].stripped_strings)
    result = _get_traits_and_specialities_dict(player_traits, player_specialities, all_traits, all_specialities)
    return result


def parse_player_miscellaneous_data(main_article):
    ul = (main_article.div
          .find('div', class_='teams', recursive=False)
          .table.tr.ul)
    attribute_dict = {}
    strings = ul.stripped_strings
    for key in strings:
        attribute_dict[key] = next(strings)
    work_rates = attribute_dict.pop('Work rate').split(' / ')
    attribute_dict['Work rate att'] = work_rates[0]
    attribute_dict['Work rate def'] = work_rates[1]
    return attribute_dict


def get_position_ratings(main_soup, main_article, all_positions):
    position_col_name = 'Position'
    ratings_div = main_soup.aside.find('div', class_='toast mb-20', recursive=False)
    if ratings_div.h5.text == 'Real overall rating':
        ratings_table = ratings_div.table
        position_ratings_df = pd.read_html(str(ratings_table))[0][[position_col_name, 'OVA']]
        split_df = (position_ratings_df[position_col_name]
                    .str.split(expand=True)
                    .assign(OVA=position_ratings_df['OVA']))
        position_ratings_dict = (
        pd.concat(split_df[[i, 'OVA']].rename(columns={i: position_col_name}) for i in range(3))
        .dropna()
        .set_index(position_col_name)
        .to_dict()['OVA'])
        position_ratings_dict.update({'GK': np.nan})
    else:
        gk_rating = main_article.div.find('div', class_='stats', recursive=False).td.span.text
        position_ratings_dict = {'GK': gk_rating, **{pos: np.nan for pos in all_positions}}
    return position_ratings_dict


def get_full_position_preferences(preferred_positions_list, all_positions):
    return {'prefers_' + pos: (pos in preferred_positions_list) for pos in all_positions}


def parse_single_player_page(html, strainer, constants):
    soup = BeautifulSoup(html, 'lxml', parse_only=strainer)
    all_traits = constants['traits']
    all_specialities = constants['specialities']
    all_positions = constants['positions']

    main_soup = _get_main_soup(soup)
    main_article = _get_main_article(main_soup)
    col3_divs = _get_col3_divs(main_article)
    main_attributes = parse_main_attributes(col3_divs)
    headline_attributes = parse_headline_attributes(soup)
    metadata = parse_player_metadata(main_article)
    _preferred_positions = metadata.pop('preferred_positions')
    traits_and_specialities = parse_traits_and_specialities(col3_divs, all_traits, all_specialities)
    miscellaneous_data = parse_player_miscellaneous_data(main_article)
    position_ratings = get_position_ratings(main_soup, main_article, all_positions)
    position_preferences = get_full_position_preferences(_preferred_positions, all_positions)
    return {**main_attributes, **headline_attributes, **metadata,
            **traits_and_specialities, **miscellaneous_data, **position_ratings,
            **position_preferences}


def id_from_url(url):
    return url.split('/')[-1]


def parse_player_detailed_data(player_htmls, constants):
    strainer = SoupStrainer(['section', 'script'])
    data = []
    for player_id, html in player_htmls.items():
        row_dict = parse_single_player_page(html, strainer, constants)
        row_dict['ID'] = id_from_url(player_id)
        data.append(row_dict)
    df = pd.DataFrame(data)
    col_order = [*constants['uncategorised'],
                 *constants['body_features'],
                 *constants['headline_attributes'],
                 *constants['special_attributes'],
                 *constants['main_attributes'],
                 *constants['positions'],
                 *constants['traits'],
                 *constants['specialities'],
                 *constants['position_preferences']]
    df = (df
          .assign(release_clause_EUR=df['Release clause'].pipe(convert_currency))
          .drop('Release clause', axis=1))
    numeric_cols_to_be_converted = ['ID', 'Height_cm', 'Weight_kg',
                                    *constants['headline_attributes'],
                                    'International reputation',
                                    'Skill moves',
                                    'Weak foot',
                                    *constants['positions']]
    for col in numeric_cols_to_be_converted:
        df.loc[:, col] = pd.to_numeric(df[col])
    return df[col_order]


def get_player_detailed_data(IDs, from_file=False):
    constants = read_constants()
    player_htmls = get_player_htmls(IDs, from_file)
    return parse_player_detailed_data(player_htmls, constants)