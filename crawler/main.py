from crawler.overview_data import get_overview_data
from crawler.player_data import get_player_detailed_data
from crawler.league_data import get_league_IDs, get_league_data
import shutil
import crawler.utils

_FEATHER_FILEPATHS = crawler.utils.filepath_tree('final', '.feather')
_CSV_FILEPATHS = crawler.utils.filepath_tree('final', '.csv')

def save_data(data, category_key):
    """Saves df to .feather and .csv"""
    version_key = 'current'
    feather_path = str(_FEATHER_FILEPATHS[version_key][category_key])
    data.to_feather(feather_path)
    csv_path = str(_CSV_FILEPATHS[version_key][category_key])
    data.to_csv(csv_path, index=False)

def move_if_exists(src, dst):
    try:
        shutil.move(src, dst)
    except FileNotFoundError:
        pass

def update_data(data, category_key):
    csv_path_current = _CSV_FILEPATHS['current'][category_key]
    csv_path_previous = _CSV_FILEPATHS['previous'][category_key]
    feather_path_current = _FEATHER_FILEPATHS['current'][category_key]
    feather_path_previous = _FEATHER_FILEPATHS['previous'][category_key]
    move_if_exists(csv_path_current, csv_path_previous)
    move_if_exists(feather_path_current, feather_path_previous)
    save_data(data, category_key)

def main(from_file=False, update_html_store=True, transfer_old_data=True):
    """Creates and exports the full dataset.
    If from_file is set to False, then the crawler will be called to get all the html from scratch.
    Then if update_html_store is set to True, the html .json files will be updated with the newly downloaded html.
    Otherwise, the pre-existing .json files containing raw html will be used.
    """

    player_overview_data = get_overview_data(from_file, update_html_store)

    IDs = player_overview_data['ID']
    league_IDs = get_league_IDs(from_file, update_html_store)
    league_data = get_league_data(league_IDs, from_file, update_html_store)
    player_detailed_data = get_player_detailed_data(IDs, from_file)
    complete_data = (player_overview_data
                     .merge(player_detailed_data, on='ID')
                     .merge(league_data, on='club', how='left'))
    if transfer_old_data:
        update_data(player_overview_data, 'overview')
        update_data(player_detailed_data, 'player')
        update_data(league_data, 'league')
        update_data(complete_data, 'complete')
    else:
        save_data(player_overview_data, 'overview')
        save_data(player_detailed_data, 'player')
        save_data(league_data, 'league')
        save_data(complete_data, 'complete')