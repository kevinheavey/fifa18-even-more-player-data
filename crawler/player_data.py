import pandas as pd
import numpy as np
from crawler.utils import parse_headline_attributes, read_constants, convert_currency, standardise_col_names
from crawler.html_download import get_player_htmls
import parsel
from multiprocessing import Pool, cpu_count


def parse_main_attributes(main_rectangle_selector):

    sub_div_selectors = main_rectangle_selector.xpath('./body/div/div')[:-1]
    # last one is traits and specialities, which we don't want here
    li_selectors = sub_div_selectors.xpath('div/ul/li')
    attribute_names = [name.strip() for name in li_selectors.xpath('text()').extract() if not name.isspace()]
    attribute_values = li_selectors.xpath('span/text()').extract()
    return dict(zip(attribute_names, attribute_values))


def parse_player_metadata(metadata_selector):

    attribute_dict = {}
    span_selector = metadata_selector.xpath('./body/div/div[1]/div/span')
    subspan_selector = span_selector.xpath('span')
    span_strings = [s.strip() for s in span_selector.xpath('text()').extract()]
    subspan_strings = [s.strip() for s in subspan_selector.xpath('text()').extract()]
    attribute_dict['full_name'] = span_strings[0].strip()
    age_height_weight = span_strings[-1].split()
    attribute_dict['Birth date'] = ' '.join(age_height_weight[2:5]).replace(',', '').strip('(').strip(')')
    attribute_dict['Height_cm'] = age_height_weight[5].strip('cm')
    attribute_dict['Weight_kg'] = age_height_weight[-1].strip('kg')
    attribute_dict['preferred_positions'] = subspan_strings

    return attribute_dict


def _get_traits_and_specialities_dict(player_traits, player_specialities, all_traits, all_specialities):
    player_traits = [str(trait) + '_trait' for trait in player_traits]
    player_specialities = [str(spec) + '_speciality' for spec in player_specialities]
    trait_dict = {trait: (trait in player_traits) for trait in all_traits}
    speciality_dict = {speciality: (speciality in player_specialities) for speciality in all_specialities}
    return {**trait_dict, **speciality_dict}


def parse_traits_and_specialities(main_rectangle_selector_list, all_traits, all_specialities):
    div_selector = main_rectangle_selector_list[1].xpath('./body/div/div[last()]/div')
    if not div_selector:
        player_traits, player_specialities = [np.nan], [np.nan]
    else:
        uls = div_selector.xpath('ul')
        n_uls = len(uls)
        # if the player has both traits and specialities, we know traits come first
        # if they only have traits or only specialities, we need to work out which
        if n_uls == 1:
            ul = uls[0]
            ul_strings = [s.strip() for s in ul.xpath('li/text()').extract()]
            ul_h5_text = ul.xpath('../h5/text()').extract_first().strip()
            if ul_h5_text == 'Traits':
                player_traits = ul_strings
                player_specialities = [np.nan]
            elif ul_h5_text == 'Specialities':
                player_traits = [np.nan]
                player_specialities = ul_strings
        else:
            player_traits = [s.strip() for s in uls[0].xpath('li/text()').extract()]
            player_specialities = [s.strip() for s in uls[1].xpath('li/text()').extract()]
    result = _get_traits_and_specialities_dict(player_traits, player_specialities, all_traits, all_specialities)
    return result


def parse_player_miscellaneous_data(metadata_selector):

    miscellaneous_info_div = metadata_selector
    ul_selector = miscellaneous_info_div.xpath('./body/div/div[3]/table/tr/td[1]/ul[1]')[0]
    strings = [x.strip() for x in ul_selector.xpath('.//text()').extract() if not x.isspace()]
    attribute_dict = dict(zip(strings[::2], strings[1::2]))
    work_rates = attribute_dict.pop('Work rate').split(' / ')
    attribute_dict['Work rate att'] = work_rates[0]
    attribute_dict['Work rate def'] = work_rates[1]
    return attribute_dict


def get_position_ratings(position_ratings_selector, metadata_selector, all_positions):
    all_outfield_positions = [pos for pos in all_positions if pos != 'GK']
    position_col_name = 'Position'
    div_selector = position_ratings_selector.xpath('./body/div')
    if div_selector.xpath('h5/text()').extract_first() == 'Real overall rating':
        ratings_table = div_selector.xpath('table').extract_first()
        position_ratings_df = pd.read_html(ratings_table)[0][[position_col_name, 'OVA']]
        split_df = (position_ratings_df[position_col_name]
                    .str.split(expand=True)
                    .assign(OVA=position_ratings_df['OVA']))
        position_ratings_dict = (split_df
                                 .melt(id_vars='OVA', value_name=position_col_name)[['OVA', position_col_name]]
                                 .dropna()
                                 .set_index(position_col_name)
                                 .to_dict()['OVA'])
        position_ratings_dict['GK'] = np.nan
    else:
        gk_rating = metadata_selector.xpath('./body/div/div[2]/table/tr/td[1]/span/text()').extract_first()
        position_ratings_dict = {'GK': gk_rating, **{pos: np.nan for pos in all_outfield_positions}}
    return position_ratings_dict


def get_full_position_preferences(preferred_positions_list, all_positions):
    return {'prefers_' + pos: (pos in preferred_positions_list) for pos in all_positions}


def parse_single_player_page(url, html_dict, constants):
    player_id = id_from_url(url)
    headline_attributes_selector = parsel.Selector(html_dict['headline_attributes'])
    # actually the line below is the first three divs under the main article
    main_article_selector_list = parsel.SelectorList(parsel.Selector(item) for item in html_dict['main'])
    metadata_selector = main_article_selector_list[0]
    main_rectangle_selector_list = main_article_selector_list[1:]
    position_ratings_selector = parsel.Selector(html_dict['position_ratings'])

    all_traits = constants['traits']
    all_specialities = constants['specialities']
    all_positions = constants['positions']

    main_attributes = parse_main_attributes(main_rectangle_selector_list)
    headline_attributes = parse_headline_attributes(headline_attributes_selector)
    metadata = parse_player_metadata(metadata_selector)
    _preferred_positions = metadata.pop('preferred_positions')
    traits_and_specialities = parse_traits_and_specialities(main_rectangle_selector_list, all_traits, all_specialities)
    miscellaneous_data = parse_player_miscellaneous_data(metadata_selector)
    position_ratings = get_position_ratings(position_ratings_selector, metadata_selector, all_positions)
    position_preferences = get_full_position_preferences(_preferred_positions, all_positions)
    return {'ID': player_id, **main_attributes, **headline_attributes, **metadata,
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
    func_args = [(url, html, constants) for url, html in player_htmls.items()]
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
    return df[col_order].rename(columns={'Release clause':'EUR_release_clause'})


def get_player_detailed_data(IDs, from_file=False, update_html_store=False):
    constants = read_constants()
    player_htmls = get_player_htmls(IDs, from_file, update_html_store)
    return parse_player_detailed_data(player_htmls, constants).pipe(standardise_col_names)