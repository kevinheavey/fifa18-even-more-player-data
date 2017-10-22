import pandas as pd
import numpy as np
from bs4 import BeautifulSoup, SoupStrainer
from .utils import parse_headline_attributes, read_constants, convert_currency
from .html_download import get_player_htmls
from multiprocessing import Pool, cpu_count

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
    player_traits = [str(trait) + '_trait' for trait in player_traits]
    player_specialities = [str(spec) + '_speciality' for spec in player_specialities]
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
    all_outfield_positions = [pos for pos in all_positions if pos != 'GK']
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
        position_ratings_dict = {'GK': gk_rating, **{pos: np.nan for pos in all_outfield_positions}}
    return position_ratings_dict


def get_full_position_preferences(preferred_positions_list, all_positions):
    return {'prefers_' + pos: (pos in preferred_positions_list) for pos in all_positions}


def parse_single_player_page(url, html, strainer, constants):

    player_id = id_from_url(url)
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
    return {'ID':player_id, **main_attributes, **headline_attributes, **metadata,
            **traits_and_specialities, **miscellaneous_data, **position_ratings,
            **position_preferences}


def id_from_url(url):
    return url.split('/')[-1]

def _feet_to_cm(str_series):
    inches_to_cm_factor = 2.54
    feet_inches_df = (str_series.str.strip('"')
                      .str.split("'", expand=True)
                      .rename(columns={0:'feet', 1:'inches'})
                      .astype('int'))
    total_inches_series = feet_inches_df['feet']*12 + feet_inches_df['inches']
    return (total_inches_series * inches_to_cm_factor).round().astype('int').rename('Height_cm')

def _convert_height_col(height_series):
    feet_index = height_series.str.contains('"')
    if feet_index.any():
        feet_series = height_series[feet_index]
        return (height_series
                .mask(feet_index, other=_feet_to_cm(feet_series))
                .pipe(pd.to_numeric, errors='ignore'))
    else:
        return height_series

def _lb_to_kg(str_series):
    lb_to_kg_factor = 0.453592
    return (str_series.str.strip('lbs')
            .astype('int')
            .mul(lb_to_kg_factor)
            .round().astype('int'))

def _convert_weight_col(weight_series):
    lb_index = weight_series.str.contains('lbs')
    if lb_index.any():
        lb_series = weight_series[lb_index]
        return (weight_series
                .mask(lb_index, other=_lb_to_kg(lb_series))
                .pipe(pd.to_numeric, errors='ignore'))
    else:
        return weight_series


def parse_player_detailed_data(player_htmls, constants):
    pool = Pool(cpu_count())
    strainer = SoupStrainer(['section', 'script'])
    # data = []
    # for url, html in player_htmls.items():
    #     row_dict = parse_single_player_page(url, html, strainer, constants)
    #     data.append(row_dict)
    func_args = [(url, html, strainer, constants) for url, html in player_htmls.items()]
    data = pool.starmap(parse_single_player_page, func_args)
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
    df.loc[:, 'Release clause'] = df['Release clause'].pipe(convert_currency)
    df.loc[:, 'Birth date'] = pd.to_datetime(df['Birth date'], format='%b %d %Y')
    df.loc[:, 'Real face'] = np.where(df['Real face']=='Yes', True, False)
    df.loc[:, 'Height_cm'] = _convert_height_col(df['Height_cm'])
    df.loc[:, 'Weight_kg'] = _convert_weight_col(df['Weight_kg'])
    numeric_cols_to_be_converted = ['ID',
                                    *constants['headline_attributes'],
                                    *constants['main_attributes'],
                                    'International reputation',
                                    'Skill moves',
                                    'Weak foot',
                                    *constants['positions']]
    for col in numeric_cols_to_be_converted:
        df.loc[:, col] = pd.to_numeric(df[col], errors='ignore') #deal with errors later
    return df[col_order].rename(columns={'Release clause':'Release_clause_EUR'})


def get_player_detailed_data(IDs, from_file=False):
    constants = read_constants()
    player_htmls = get_player_htmls(IDs, from_file)
    return parse_player_detailed_data(player_htmls, constants)