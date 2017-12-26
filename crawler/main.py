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
    data.to_csv(csv_path, index=False, encoding='utf_8')

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

def get_complete_data(player_overview_data, league_data, player_detailed_data):
    col_order = ['ID', 'name', 'full_name', 'club', 'club_logo', 'special',
                 'age', 'league', 'birth_date', 'height_cm', 'weight_kg',
                 'body_type', 'real_face', 'flag', 'nationality', 'photo', 'eur_value',
                 'eur_wage', 'eur_release_clause', 'overall', 'potential',
                 'pac', 'sho', 'pas', 'dri', 'def', 'phy', 'international_reputation',
                 'skill_moves', 'weak_foot', 'work_rate_att', 'work_rate_def',
                 'preferred_foot', 'crossing', 'finishing', 'heading_accuracy',
                 'short_passing', 'volleys', 'dribbling', 'curve',
                 'free_kick_accuracy', 'long_passing', 'ball_control', 'acceleration',
                 'sprint_speed', 'agility', 'reactions', 'balance', 'shot_power',
                 'jumping', 'stamina', 'strength', 'long_shots', 'aggression',
                 'interceptions', 'positioning', 'vision', 'penalties',
                 'composure', 'marking', 'standing_tackle', 'sliding_tackle',
                 'gk_diving', 'gk_handling', 'gk_kicking', 'gk_positioning',
                 'gk_reflexes', 'rs', 'rw', 'rf', 'ram', 'rcm', 'rm', 'rdm',
                 'rcb', 'rb', 'rwb', 'st', 'lw', 'cf', 'cam', 'cm', 'lm', 'cdm',
                 'cb', 'lb', 'lwb', 'ls', 'lf', 'lam', 'lcm', 'ldm', 'lcb', 'gk',
                 '1_on_1_rush_trait', 'acrobatic_clearance_trait', 'argues_with_officials_trait',
                 'avoids_using_weaker_foot_trait', 'backs_into_player_trait', 'bicycle_kicks_trait',
                 'cautious_with_crosses_trait', 'chip_shot_trait', 'chipped_penalty_trait',
                 'comes_for_crosses_trait', 'corner_specialist_trait', 'diver_trait',
                 'dives_into_tackles_trait', 'diving_header_trait', 'driven_pass_trait',
                 'early_crosser_trait', "fan's_favourite_trait", 'fancy_flicks_trait',
                 'finesse_shot_trait', 'flair_trait', 'flair_passes_trait',
                 'gk_flat_kick_trait', 'gk_long_throw_trait', 'gk_up_for_corners_trait',
                 'giant_throw_in_trait', 'inflexible_trait', 'injury_free_trait',
                 'injury_prone_trait', 'leadership_trait', 'long_passer_trait',
                 'long_shot_taker_trait', 'long_throw_in_trait', 'one_club_player_trait',
                 'outside_foot_shot_trait', 'playmaker_trait', 'power_free_kick_trait',
                 'power_header_trait', 'puncher_trait', 'rushes_out_of_goal_trait',
                 'saves_with_feet_trait', 'second_wind_trait', 'selfish_trait',
                 'skilled_dribbling_trait', 'stutter_penalty_trait', 'swerve_pass_trait',
                 'takes_finesse_free_kicks_trait', 'target_forward_trait', 'team_player_trait',
                 'technical_dribbler_trait', 'tries_to_beat_defensive_line_trait',
                 'poacher_speciality', 'speedster_speciality', 'aerial_threat_speciality',
                 'dribbler_speciality', 'playmaker_speciality', 'engine_speciality',
                 'distance_shooter_speciality', 'crosser_speciality',
                 'free_kick_specialist_speciality', 'tackling_speciality', 'tactician_speciality',
                 'acrobat_speciality', 'strength_speciality', 'clinical_finisher_speciality',
                 'prefers_rs', 'prefers_rw', 'prefers_rf', 'prefers_ram', 'prefers_rcm',
                 'prefers_rm', 'prefers_rdm', 'prefers_rcb', 'prefers_rb', 'prefers_rwb',
                 'prefers_st', 'prefers_lw', 'prefers_cf', 'prefers_cam', 'prefers_cm',
                 'prefers_lm', 'prefers_cdm', 'prefers_cb', 'prefers_lb', 'prefers_lwb',
                 'prefers_ls', 'prefers_lf', 'prefers_lam', 'prefers_lcm', 'prefers_ldm',
                 'prefers_lcb', 'prefers_gk']
    complete_data = (player_overview_data
                     .merge(league_data, on='club', how='left')
                     .merge(player_detailed_data, on='ID'))[col_order]
    return complete_data


def main(from_file=False, update_html_store=True, transfer_old_data=True):
    """Creates and exports the full dataset.

    Parameters
    ----------
    from_file : Boolean, default False
        If set to False, then the crawler will be called to download all the html from scratch.
    update_html_store : Boolean, default True
        If set to True and from_file is set to false, the html .json files will be updated with the newly downloaded html.
        Otherwise, the pre-existing .json files containing raw html will be used.
    transfer_old_data: Boolean, default True
        Move the data in crawler/final/current to crawler/final/previous, instead of just overwriting it.
    """

    player_overview_data = get_overview_data(from_file, update_html_store)

    IDs = player_overview_data['ID']
    league_IDs = get_league_IDs(from_file, update_html_store)
    league_data = get_league_data(league_IDs, from_file, update_html_store)
    player_detailed_data = get_player_detailed_data(IDs, from_file)
    complete_data = get_complete_data(player_overview_data, league_data, player_detailed_data)
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