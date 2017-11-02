from multiprocessing import Pool, cpu_count
import pandas as pd
from bs4 import BeautifulSoup, SoupStrainer
from crawler.html_download import get_league_overview_html, get_league_htmls

def get_league_IDs(from_file=False, update_files=False):
    html = get_league_overview_html(from_file, update_files)
    strainer = SoupStrainer('tbody')
    soup = BeautifulSoup(html, 'lxml', parse_only=strainer)
    league_id_dict = {}
    for row in soup.tbody.find_all('tr', recursive=False):
        league_hyperlink = row.find_all('td', recursive=False)[1].a
        league_id = league_hyperlink['href'].split('/')[-1]
        # the league name contains a (number) at the end to indicate what level the league is
        # e.g. English Championship (2)
        # we're currently getting rid of this but we may later decide to use it
        league_name = league_hyperlink.text.rsplit(' ', maxsplit=1)[0]
        league_id_dict[league_id] = league_name
    return league_id_dict


def parse_single_league_page(html, strainer, league_name):
    soup = BeautifulSoup(html, 'lxml', parse_only=strainer)
    club_names = []
    for row in soup.tbody.find_all('tr', recursive=False):
        club_names.append(row.td.a.text)
    return [{'club': club, 'league':league_name} for club in club_names]

def parse_league_data(league_htmls_dict, league_id_dict):
    strainer = SoupStrainer('tbody')
    league_name_html_dict = {}
    for url, html in league_htmls_dict.items():
        ID = url.split('/')[-1]
        league_name = league_id_dict[ID]
        league_name_html_dict[league_name] = html
    page_dict_lists = []
    for league_name, html in league_name_html_dict.items():
        page_dicts = parse_single_league_page(html, strainer, league_name)
        page_dict_lists.append(page_dicts)
    data = []
    for sub_list in page_dict_lists:
        data.extend(sub_list)
    return pd.DataFrame(data)

def get_league_data(league_IDs, from_file=False, update_html_store=False):
    league_htmls_dict = get_league_htmls(league_IDs, from_file, update_html_store)
    return parse_league_data(league_htmls_dict, league_IDs)
