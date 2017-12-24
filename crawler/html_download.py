import aiohttp
import asyncio
import requests
import json
import shutil
import gzip
import parsel
import crawler.utils

_JSON_FILEPATHS = crawler.utils.filepath_tree('html_jsons', '.json.gz')


async def _fetch(session, url):
    async with session.get(url, timeout=60 * 60) as response:
        return await response.text()


async def _fetch_all(session, urls, loop):
    results = await asyncio.gather(
        *[_fetch(session, url) for url in urls],
        return_exceptions=True  # so we can deal with exceptions later
    )

    return results


def _get_htmls_from_json(category_key):
    with gzip.open(_JSON_FILEPATHS['current'][category_key], 'rb') as f:
        htmls = json.load(f)
    return htmls


def _get_htmls(urls, category_key, from_file=False):
    if from_file:
        result = _get_htmls_from_json(category_key)
    else:
        if len(urls) > 1:
            loop = asyncio.get_event_loop()
            connector = aiohttp.TCPConnector(limit=100)
            with aiohttp.ClientSession(loop=loop, connector=connector) as session:
                htmls = loop.run_until_complete(_fetch_all(session, urls, loop))
            raw_result = dict(zip(urls, htmls))
        else:
            raw_result = requests.get(urls[0]).text
        result = _filter_htmls(raw_result, category_key)
    return result


def _get_relevant_overview_html(overview_html):
    selector = parsel.Selector(text=overview_html)
    table_selector = selector.xpath('/html/body/section/section/article/div/table')
    return table_selector.extract_first()

def _get_relevant_player_html_dict(player_html):
    selector = parsel.Selector(text=player_html)
    body = selector.xpath('/html/body')
    section1 = body.xpath('section/section[1]')
    return {'headline_attributes':body.xpath('script[1]').extract_first(),
            'position_ratings':section1.xpath('aside/div[2]').extract_first(),
            'main':section1.xpath('article/div[position() < 4]').extract()}

def _get_relevant_league_html(league_html):
    selector = parsel.Selector(text=league_html)
    tbody_selector = selector.xpath('/html/body/section/section/aside/div[2]/table/tbody')
    return tbody_selector.extract_first()

def _get_relevant_league_overview_html(league_overview_html):
    selector = parsel.Selector(text=league_overview_html)
    table_selector = selector.xpath('/html/body/section/section/article/table')
    return table_selector.extract_first()

def _filter_htmls(raw_htmls, category_key):
    if category_key == 'league_overview':
        return _get_relevant_league_overview_html(raw_htmls)
    dispatch_dict = {'player':_get_relevant_player_html_dict,
                     'overview':_get_relevant_overview_html,
                     'league':_get_relevant_league_html}
    filtered_html_dict = {}
    for url, html in raw_htmls.items():
        relevant_html = dispatch_dict[category_key](html)
        filtered_html_dict[url] = relevant_html
    return filtered_html_dict

def save_htmls_to_json(htmls, file_key):
    with gzip.open(_JSON_FILEPATHS['current'][file_key], 'wb') as f:
        f.write(json.dumps(htmls).encode())


def update_htmls_jsons(new_htmls, category_key):
    try:
        shutil.move(_JSON_FILEPATHS['current'][category_key],
                    _JSON_FILEPATHS['previous'][category_key])
    except FileNotFoundError:
        pass
    save_htmls_to_json(new_htmls, category_key)


def update_overview_htmls_jsons(overview_htmls):
    file_key = 'overview'
    update_htmls_jsons(overview_htmls, file_key)


def update_player_htmls_jsons(player_htmls):
    file_key = 'player'
    update_htmls_jsons(player_htmls, file_key)


def update_league_htmls_jsons(league_htmls):
    file_key = 'league'
    update_htmls_jsons(league_htmls, file_key)


def update_league_overview_html_json(league_overview_html):
    file_key = 'league_overview'
    update_htmls_jsons(league_overview_html, file_key)


def get_overview_urls():
    urls = []
    base_url = "https://sofifa.com/players?offset="
    offset_increment = 80
    for i in range(226):  # WARNING: this may not be invariant
        url = base_url + str(i * offset_increment)
        urls.append(url)
    return urls


def get_overview_htmls(from_file=False, update_files=False):
    urls = get_overview_urls()
    overview_htmls = _get_htmls(urls, category_key='overview', from_file=from_file)
    if update_files and not from_file:
        update_overview_htmls_jsons(overview_htmls)
    return overview_htmls


def get_player_urls(IDs):
    urls = []
    base_url = 'https://sofifa.com/player/'
    for ID in IDs:
        url = base_url + str(ID)
        urls.append(url)
    return urls


def get_player_htmls(IDs, from_file=False, update_files=False):
    if from_file:
        urls = None
    else:
        urls = get_player_urls(IDs)
    player_htmls = _get_htmls(urls, category_key='player', from_file=from_file)
    if update_files and not from_file:
        update_player_htmls_jsons(player_htmls)
    return player_htmls


def get_league_overview_html(from_file=False, update_files=False):
    url = 'https://sofifa.com/leagues'
    html = _get_htmls([url], category_key='league_overview', from_file=from_file)
    if update_files and not from_file:
        update_league_overview_html_json(html)
    return html

def get_league_htmls(league_IDs, from_file=False, update_files=False):
    base_url = 'https://sofifa.com/league/'
    urls = [base_url + str(ID) for ID in league_IDs]
    league_htmls = _get_htmls(urls, category_key='league', from_file=from_file)
    if update_files and not from_file:
        update_league_htmls_jsons(league_htmls)
    return league_htmls
