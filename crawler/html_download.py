import aiohttp
import asyncio
from pathlib import Path
import pickle
import shutil

_PICKLE_DIR = Path(__file__).parents[1] / 'data/pickled_html'
_PICKLE_SUBDIRS = {'overview': _PICKLE_DIR / 'overview',
                  'player': _PICKLE_DIR / 'player'}
_PICKLE_FILEPATHS = {'overview': _PICKLE_SUBDIRS['overview'] / 'current.pkl',
                    'player': _PICKLE_SUBDIRS['player'] / 'current.pkl'}

async def _fetch(session, url):
    with aiohttp.Timeout(30):
        async with session.get(url) as response:
            return await response.text()


async def _fetch_all(session, urls, loop):
    results = await asyncio.gather(
        *[_fetch(session, url) for url in urls],
        return_exceptions=True  # so we can deal with exceptions later
    )

    return results


def _get_htmls_from_pickle(file_key):
    with open(_PICKLE_FILEPATHS[file_key], 'rb') as f:
        htmls = pickle.load(f)
    return htmls


def _get_htmls(urls, from_file=False, file_key=None):
    if from_file:
        return _get_htmls_from_pickle(file_key)
    else:
        loop = asyncio.get_event_loop()
        with aiohttp.ClientSession(loop=loop) as session:
            htmls = loop.run_until_complete(_fetch_all(session, urls, loop))
    return dict(zip(urls, htmls))

def save_htmls_to_pickle(htmls, file_key):
    with open(_PICKLE_FILEPATHS[file_key], 'wb') as f:
        pickle.dump(htmls, f)

def update_pickled_htmls(new_htmls, file_key):
    try:
        shutil.move(_PICKLE_FILEPATHS[file_key],
                    _PICKLE_SUBDIRS[file_key] / 'previous.pkl')
    except FileNotFoundError:
        pass
    save_htmls_to_pickle(new_htmls, file_key)

def update_pickled_overview_htmls(overview_htmls):
    file_key = 'overview'
    update_pickled_htmls(overview_htmls, file_key)

def update_pickled_player_htmls(player_htmls):
    file_key = 'player'
    update_pickled_htmls(player_htmls, file_key)

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
        update_pickled_player_htmls(player_htmls)
    return player_htmls

def get_overview_htmls(from_file=False, update_files=False):
    urls = get_overview_urls()
    overview_htmls = _get_htmls(urls, from_file, file_key='overview')
    if update_files and not from_file:
        update_pickled_overview_htmls(overview_htmls)
    return overview_htmls