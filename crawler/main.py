from crawler.overview_data import get_overview_data
from crawler.player_data import get_player_detailed_data
from pathlib import Path
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

def main(from_file=False, update_files=True):

    player_overview_data = get_overview_data(from_file=from_file,
                                             update_files=update_files)
    save_data(player_overview_data, 'overview')
    IDs = player_overview_data['ID']
    player_detailed_data = get_player_detailed_data(IDs, from_file)
    save_data(player_detailed_data, 'player')
    complete_data = player_overview_data.merge(player_detailed_data, on='ID')
    save_data(complete_data, 'complete')