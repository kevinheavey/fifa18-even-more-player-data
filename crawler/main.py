from crawler.overview_data import get_overview_data
from crawler.player_data import get_player_detailed_data
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

def update_data(data, category_key):
    csv_path_current = _CSV_FILEPATHS['current'][category_key]
    csv_path_previous = _CSV_FILEPATHS['previous'][category_key]
    feather_path_current = _FEATHER_FILEPATHS['current'][category_key]
    feather_path_previous = _FEATHER_FILEPATHS['previous'][category_key]
    shutil.move(csv_path_current, csv_path_previous)
    shutil.move(feather_path_current, feather_path_previous)
    save_data(data, category_key)

def main(from_file=False, update_html_store=True):
    """Creates and exports the full dataset.
    If from_file is set to False, then the crawler will be called to get all the html from scratch.
    Then if update_html_store is set to True, the html .json files will be updated with the newly downloaded html.
    Otherwise, the pre-existing .json files containing raw html will be used.
    """

    player_overview_data = get_overview_data(from_file=from_file,
                                             update_files=update_html_store)
    save_data(player_overview_data, 'overview')
    IDs = player_overview_data['ID']
    player_detailed_data = get_player_detailed_data(IDs, from_file)
    save_data(player_detailed_data, 'player')
    complete_data = player_overview_data.merge(player_detailed_data, on='ID')
    save_data(complete_data, 'complete')