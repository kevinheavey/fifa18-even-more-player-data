from bs4 import BeautifulSoup, SoupStrainer
import pandas as pd
import numpy as np
from multiprocessing import Pool, cpu_count
from .html_download import get_overview_htmls

def parse_single_row(overview_table_row):

    record_dict = {}
    tds = overview_table_row.find_all('td', recursive=False)
    record_dict['Photo'] = tds[0].find('img').get('data-src')
    record_dict['ID'] = tds[0].find('img').get('id')
    record_dict['Nationality'] = tds[1].find('a').get('title')
    record_dict['Flag'] = tds[1].find('img').get('data-src')
    record_dict['Name'] = tds[1].find_all('a')[1].text
    record_dict['Age'] = tds[2].find('div').text.strip()
    record_dict['Overall'] = tds[3].text.strip()
    record_dict['Potential'] = tds[4].text.strip()
    record_dict['Club'] = tds[5].find('a').text
    record_dict['Club logo'] = tds[5].find('img').get('data-src')
    record_dict['Value'] = tds[7].text
    record_dict['Wage'] = tds[8].text
    record_dict['Special'] = tds[17].text

    return record_dict


def parse_single_overview_page(html, strainer):
    soup = BeautifulSoup(html, 'lxml', parse_only=strainer)
    row_dicts = []
    for row in soup.tbody.find_all('tr', recursive=False):
        row_dicts.append(parse_single_row(row))
    return row_dicts


def parse_overview_data(overview_htmls):
    pool = Pool(cpu_count())
    strainer = SoupStrainer('tbody')
    htmls = list(overview_htmls.values())
    func_args = [(h, strainer) for h in htmls]
    data_lists = pool.starmap(parse_single_overview_page, func_args)
    data = []
    for sub_list in data_lists:
        data.extend(sub_list)
    return pd.DataFrame(data)

def convert_currency(curr_col):
    without_euro_symbol = curr_col.str[1:]
    unit_symbol = without_euro_symbol.str[-1]
    numeric_part = np.where(unit_symbol == '0', 0, without_euro_symbol.str[:-1].pipe(pd.to_numeric))
    multipliers = unit_symbol.replace({'M':1e6, 'K':1e3}).pipe(pd.to_numeric)
    return numeric_part * multipliers

def clean_overview_data(df):
    return (df.drop_duplicates('ID')
            .assign(EUR_value = lambda df: df['Value'].pipe(convert_currency),
                                EUR_wage = lambda df: df['Wage'].pipe(convert_currency))
            .drop(['Value', 'Wage'], axis=1))

def get_overview_data(from_file=False, update_files=True):
    overview_htmls = get_overview_htmls(from_file, update_files)
    return parse_overview_data(overview_htmls).pipe(clean_overview_data)