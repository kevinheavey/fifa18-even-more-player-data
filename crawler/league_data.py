import pandas as pd
import parsel
from crawler.html_download import get_league_overview_html, get_league_htmls

def get_league_IDs(from_file=False, update_html_store=False):
    html = get_league_overview_html(from_file, update_html_store)
    selector = parsel.Selector(html)
    league_id_dict = {}
    for row_selector in selector.xpath('./body/table/tbody/tr'):
        league_hyperlink_selector = row_selector.xpath('td[2]/a')
        league_id = league_hyperlink_selector.xpath('@href').extract_first().split('/')[-1]
        # the league name contains a (number) at the end to indicate what level the league is
        # e.g. English Championship (2)
        # we're currently getting rid of this but we may later decide to use it
        league_name = league_hyperlink_selector.xpath('text()').extract_first().rsplit(' ', maxsplit=1)[0]
        league_id_dict[league_id] = league_name
    return league_id_dict


def parse_single_league_page(html, league_name):
    selector = parsel.Selector(html)
    club_names = selector.xpath('./body/tbody/tr/td/a/text()').extract()
    return [{'club': club, 'league':league_name} for club in club_names]

def parse_league_data(league_htmls_dict, league_id_dict):
    league_name_html_dict = {}
    for url, html in league_htmls_dict.items():
        ID = url.split('/')[-1]
        league_name = league_id_dict[ID]
        league_name_html_dict[league_name] = html
    page_dict_lists = []
    for league_name, html in league_name_html_dict.items():
        page_dicts = parse_single_league_page(html, league_name)
        page_dict_lists.append(page_dicts)
    data = []
    for sub_list in page_dict_lists:
        data.extend(sub_list)
    return pd.DataFrame(data)

def get_league_data(league_IDs, from_file=False, update_html_store=False):
    league_htmls_dict = get_league_htmls(league_IDs, from_file, update_html_store)
    return parse_league_data(league_htmls_dict, league_IDs)
