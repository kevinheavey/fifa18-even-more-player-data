from crawler.overview_data import get_overview_data
from crawler.player_data import get_player_detailed_data
from pathlib import Path
FINAL_DATA_DIR = Path(__file__).parents[1] / 'data/final'

def save_data(data, name):
    feather_path = str((FINAL_DATA_DIR / name).with_suffix('.feather'))
    data.to_feather(feather_path)
    csv_path = str((FINAL_DATA_DIR / name).with_suffix('.csv'))
    data.to_csv(csv_path, index=False)

def main(from_file=False, update_files=True):

    player_overview_data = get_overview_data(from_file=from_file,
                                             update_files=update_files)
    save_data(player_overview_data)
    IDs = player_overview_data['ID']
    player_detailed_data = get_player_detailed_data(IDs, from_file)
    save_data(player_detailed_data)
    full_data = player_overview_data.merge(player_detailed_data, on='ID')
    save_data(full_data)