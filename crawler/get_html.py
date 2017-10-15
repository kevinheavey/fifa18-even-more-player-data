import aiohttp
import asyncio
from pathlib import Path
import pickle
import shutil

PICKLE_DIR = Path(__file__).parents[1] / 'data/pickled_html'
PICKLE_SUBDIRS = {'overview':PICKLE_DIR / 'overview',
                  'player':PICKLE_DIR / 'player'}
PICKLE_FILEPATHS = {'overview':PICKLE_SUBDIRS['overview'] / 'current.pkl',
                    'player':PICKLE_SUBDIRS['player'] / 'current.pkl'}

async def fetch(session, url):
    with aiohttp.Timeout(30):
        async with session.get(url) as response:
            return await response.text()


async def fetch_all(session, urls, loop):
    results = await asyncio.gather(
        *[fetch(session, url) for url in urls],
        return_exceptions=True  # so we can deal with exceptions later
    )

    return results


def get_htmls_from_pickle(file_key):
    with open(PICKLE_FILEPATHS[file_key], 'rb') as f:
        htmls = pickle.load(f)
    return htmls


def get_htmls(urls, from_file=False, file_key=None):
    if from_file:
        return get_htmls_from_pickle(file_key)
    else:
        loop = asyncio.get_event_loop()
        with aiohttp.ClientSession(loop=loop) as session:
            htmls = loop.run_until_complete(fetch_all(session, urls, loop))
    return dict(zip(urls, htmls))

def save_htmls_to_pickle(htmls, file_key):
    with open(PICKLE_FILEPATHS[file_key], 'wb') as f:
        pickle.dump(htmls, f)

def update_pickled_htmls(new_htmls, file_key):
    shutil.move(PICKLE_FILEPATHS[file_key], PICKLE_SUBDIRS[file_key] / 'previous.pkl')
    save_htmls_to_pickle(new_htmls, file_key)

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


def get_player_htmls(IDs, from_file=False):
    urls = get_player_urls(IDs)
    return get_htmls(urls, from_file, file_key='player')


def get_overview_htmls(from_file=False):
    urls = get_overview_urls()
    return get_htmls(urls, from_file, file_key='overview')