from multiprocessing import Pool, cpu_count
import pandas as pd
from bs4 import BeautifulSoup, SoupStrainer

def parse_single_row(row):
    stripped_strings = list(row.div.stripped_strings)
    team_name = stripped_strings[0]
    # each league name contains a division number inside parentheses at the end e.g. English Championship (2)
    # we're currently removing that
    league_name_raw = stripped_strings[1]
    league_name = league_name_raw[:league_name_raw.find('(')-1]
    return {'club':team_name, 'league':league_name}

def parse_single_team_overview_page(html, strainer):
    soup = BeautifulSoup(html, 'lxml', parse_only=strainer)
    page_dicts = []
    for row in soup.tbody.find_all('tr', recursive=False):
        page_dicts.append(parse_single_row(row))
    return page_dicts

def parse_team_overview_data(team_overview_htmls):
    pool = Pool(cpu_count())
    strainer = SoupStrainer('tbody')
    htmls = list(team_overview_htmls.values())
    func_args = [(h, strainer) for h in htmls]
    page_dict_lists = pool.starmap(parse_single_team_overview_page, func_args)
    data = []
    for sub_list in page_dict_lists:
        data.extend(sub_list)
    return pd.DataFrame(data)