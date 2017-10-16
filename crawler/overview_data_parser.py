from bs4 import BeautifulSoup, SoupStrainer
import pandas as pd

def parse_single_row(overview_table_row):

    record_dict = {}
    tds = overview_table_row.find_all('td', recursive=False)
    record_dict['photo'] = tds[0].find('img').get('data-src')
    record_dict['ID'] = tds[0].find('img').get('id')
    record_dict['nationality'] = tds[1].find('a').get('title')
    record_dict['flag'] = tds[1].find('img').get('data-src')
    record_dict['name'] = tds[1].find_all('a')[1].text
    record_dict['age'] = tds[2].find('div').text.strip()
    record_dict['overall'] = tds[3].text.strip()
    record_dict['potential'] = tds[4].text.strip()
    record_dict['club'] = tds[5].find('a').text
    record_dict['club_logo'] = tds[5].find('img').get('data-src')
    record_dict['value'] = tds[7].text
    record_dict['wage'] = tds[8].text
    record_dict['special'] = tds[17].text

    return record_dict


def parse_single_overview_page(html, strainer):
    soup = BeautifulSoup(html, 'lxml', parse_only=strainer)
    row_dicts = []
    for row in soup.tbody.find_all('tr', recursive=False):
        row_dicts.append(parse_single_row(row))
    return row_dicts


def parse_overview_data(overview_htmls):
    strainer = SoupStrainer('tbody')
    data = []
    for html in overview_htmls.values():
        row_dicts = parse_single_overview_page(html, strainer)
        data.extend(row_dicts)
    return pd.DataFrame.from_dict(data)