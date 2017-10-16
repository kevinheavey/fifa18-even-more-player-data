from .overview_data import get_overview_data
from .player_data import get_player_detailed_data

def main(from_file=False, update_files=True):

    player_overview_data = get_overview_data(from_file=from_file,
                                             update_files=update_files)
    IDs = player_overview_data['ID']
    player_detailed_data = get_player_detailed_data(IDs, from_file)
    merged = player_overview_data.merge(player_detailed_data, on='ID')
    return merged