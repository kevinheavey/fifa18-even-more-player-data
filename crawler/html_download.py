import aiohttp
import asyncio
import json
import shutil
import crawler.utils

_JSON_FILEPATHS = crawler.utils.filepath_tree('html_jsons', '.json')

async def _fetch(session, url):
    async with session.get(url, timeout=60*60) as response:
        return await response.text()


async def _fetch_all(session, urls, loop):
    results = await asyncio.gather(
        *[_fetch(session, url) for url in urls],
        return_exceptions=True  # so we can deal with exceptions later
    )

    return results


def _get_htmls_from_json(category_key):
    with open(_JSON_FILEPATHS['current'][category_key], 'r') as f:
        htmls = json.load(f)
    return htmls


def _get_htmls(urls, from_file=False, file_key=None):
    if from_file:
        return _get_htmls_from_json(file_key)
    else:
        loop = asyncio.get_event_loop()
        connector = aiohttp.TCPConnector(limit=100)
        with aiohttp.ClientSession(loop=loop, connector=connector) as session:
            htmls = loop.run_until_complete(_fetch_all(session, urls, loop))
    return dict(zip(urls, htmls))

def save_htmls_to_json(htmls, file_key):
    with open(_JSON_FILEPATHS[file_key], 'w') as f:
        json.dump(htmls, f)

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

def get_overview_urls():

    urls = []
    base_url = "https://sofifa.com/players?offset="
    offset_increment = 80
    for i in range(226):  # WARNING: this may not be invariant
        url = base_url + str(i * offset_increment)
        urls.append(url)
    return urls

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
    player_htmls = _get_htmls(urls, from_file, file_key='player')
    if update_files and not from_file:
        update_player_htmls_jsons(player_htmls)
    return player_htmls

def get_overview_htmls(from_file=False, update_files=False):
    urls = get_overview_urls()
    overview_htmls = _get_htmls(urls, from_file, file_key='overview')
    if update_files and not from_file:
        update_overview_htmls_jsons(overview_htmls)
    return overview_htmls