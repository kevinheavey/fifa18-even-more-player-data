from multiprocessing import Pool, cpu_count
import pandas as pd
import numpy as np
import parsel
from crawler.utils import convert_currency, standardise_col_names
from crawler.html_download import get_overview_htmls


def parse_single_row(row_selector):

    record_dict = {}
    td_selectors = row_selector.xpath('td')
    popover_div = td_selectors[0].xpath('div')
    if popover_div:
        player_img_selector = popover_div.xpath('figure/img')
    else:
        player_img_selector = td_selectors[0].xpath('figure/img')
    record_dict['Photo'] = player_img_selector.xpath('@data-src').extract_first().replace('/48/', '/')
    record_dict['ID'] = player_img_selector.xpath('@id').extract_first()
    namecol_hyperlink_selectors = td_selectors[1].xpath('div/a')
    record_dict['Nationality'] = namecol_hyperlink_selectors[0].xpath('@title').extract_first()
    record_dict['Flag'] = namecol_hyperlink_selectors[0].xpath('img/@data-src').extract_first().replace('.p', '@3x.p')
    record_dict['Name'] = namecol_hyperlink_selectors[1].xpath('text()').extract_first()
    record_dict['Age'] = td_selectors[2].xpath('div/text()').extract_first().strip()
    record_dict['Overall'] = td_selectors[3].xpath('div/span/text()').extract_first().strip()
    record_dict['Potential'] = td_selectors[4].xpath('div/span/text()').extract_first().strip()
    record_dict['Club'] = td_selectors[5].xpath('div/a/text()').extract_first()
    club_logo = td_selectors[5].xpath('div/figure/img/@data-src').extract_first()
    if club_logo:
        record_dict['Club logo'] = club_logo.replace('/24/', '/')
    else:
        record_dict['Club logo'] = club_logo
    record_dict['Value'] = td_selectors[7].xpath('div/text()').extract_first()
    record_dict['Wage'] = td_selectors[8].xpath('div/text()').extract_first()
    record_dict['Special'] = td_selectors[17].xpath('div/mark/text()').extract_first()

    return record_dict


def parse_single_overview_page(html):
    table_selector = parsel.Selector(html)
    row_dicts = []
    for row_selector in table_selector.xpath('./body/table/tbody/tr'):
        row_dicts.append(parse_single_row(row_selector))
    return row_dicts


def parse_overview_data(overview_htmls):
    pool = Pool(cpu_count())
    htmls = list(overview_htmls.values())
    data_lists = pool.map(parse_single_overview_page, htmls)
    data = []
    for sub_list in data_lists:
        data.extend(sub_list)
    return pd.DataFrame(data)


def clean_overview_data(df):
    return (df.drop_duplicates('ID')
            .assign(EUR_value = lambda df: df['Value'].pipe(convert_currency),
                    EUR_wage = lambda df: df['Wage'].pipe(convert_currency))
            .drop(['Value', 'Wage'], axis=1))

def get_overview_data(from_file=False, update_html_store=False):
    overview_htmls = get_overview_htmls(from_file, update_html_store)
    df = parse_overview_data(overview_htmls).pipe(clean_overview_data)
    numeric_cols_to_be_converted = ['ID', 'Overall', 'Potential',
                                    'Special', 'Age']
    for col in numeric_cols_to_be_converted:
        df.loc[:, col] = pd.to_numeric(df[col])
    return (df[['ID', 'Name', 'Club', 'Club logo', 'Flag', 'Photo', 'Nationality',
               'EUR_value', 'EUR_wage', 'Overall', 'Potential',
               'Special', 'Age']]
            .reset_index(drop=True)
            .replace({'Club':{'':np.nan}})
            .pipe(standardise_col_names))