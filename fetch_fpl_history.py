import json
import os
import sys
import time
import warnings
import logging
import pandas as pd
import requests
from common import DATA_DIR
from pprint import pprint as pp
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
LOGGER = logging.getLogger(__name__)



def fetch_player_history(player_id):
    """ Fetch JSON of a single player's FPL history. """
    url = 'https://fantasy.premierleague.com/api/element-summary/{}/'.format(player_id)
    LOGGER.debug(url)
    r = requests.get(url)
    if r.ok:
        return r.json()['history_past']


def fetch_all_player_histories(max_id=490):
    """ Fetch the histories of all players. """
    histories = []
    for player_id in range(1, max_id+1):
        try:
            history = fetch_player_history(player_id)
            if history:
                histories += history
        except json.decoder.JSONDecodeError:
            print('\nLast player found at id = {0}'.format(player_id - 1))
            return histories
        #time.sleep(0.25)  # Don't overload their servers
    else:
        warnings.warn('Last player_id not reached. You ought to try again '
                      'with a higher max_id')
    extend = alpha()
    histories += extend
    return histories

def alpha():
    url = 'https://fantasy.premierleague.com/api/bootstrap-static/'
    r = requests.get(url).json()
    data = []
    for asset in r["elements"]:
        player = {
            "assists": asset["assists"],
            "bonus": asset["bonus"],
            "bps": asset["bps"],
            "clean_sheets": asset["clean_sheets"],
            "creativity": asset["creativity"],
            "element_code": asset["code"],
            "end_cost": asset["now_cost"],
            "goals_conceded": asset["goals_conceded"],
            "goals_scored": asset["goals_scored"],
            "ict_index": asset["ict_index"],
            "influence": asset["influence"],
            "minutes": asset["minutes"],
            "own_goals": asset["own_goals"],
            "penalties_missed": asset["penalties_missed"],
            "penalties_saved": asset["penalties_saved"],
            "red_cards": asset["red_cards"],
            "saves": asset["saves"],
            "season_name": "2019/20",
            "start_cost" : asset["now_cost"],
            "threat": asset["threat"],
            "total_points": asset["total_points"],
            "yellow_cards": asset["yellow_cards"],
        }
        data.append(player)
    return data


def fetch_positions():
    """ Fetch table mapping position_ids to position names. """
    url = 'https://fantasy.premierleague.com/api/bootstrap-static/'
    r = requests.get(url)
    positions = r.json()['element_types']
    return positions


def fetch_player_info():
    """ Fetch player info for the most recent season. """
    url = 'https://fantasy.premierleague.com/api/bootstrap-static/'
    r = requests.get(url)
    positions = []
    for player in r.json()['elements']:
        positions.append({
            'position_id': player['element_type'],
            'player_id': player['code'],
            'team_id': player['team_code'],
            'full_name': player['first_name'] + ' ' + player['second_name'],
            'now_cost': player['now_cost'],
            'selected_by': player['selected_by_percent']
        })
    return positions


def fetch_and_save_history(max_id=490):
    """ Fetch and save all historical seasons. """
    scores = pd.DataFrame(fetch_all_player_histories(max_id))
    players = pd.DataFrame(fetch_player_info())
    positions = pd.DataFrame(fetch_positions())

    # Add position info and clean up columns
    history = scores.merge(players, how='outer',
                           left_on='element_code', right_on='player_id')
    history = history.merge(positions, how='outer',
                            left_on='position_id', right_on='id')

    columns = ['player_id', 'full_name', 'team_id', 'singular_name',
               'start_cost', 'end_cost', 'now_cost', 'total_points', 'season_name',
               'minutes', 'bonus', 'bps', 'goals_scored', 'assists', 'selected_by',
               'goals_conceded', 'clean_sheets', 'yellow_cards', 'red_cards',
               'penalties_missed', 'saves', 'penalties_saved']
    history = history[columns]
    history = history.rename(columns={
        'singular_name': 'position',
        'bps': 'bonus_points'
    })

    positions.to_csv(os.path.join(DATA_DIR, 'positions.csv'),
                     index=False, encoding='utf-8')
    history.to_csv(os.path.join(DATA_DIR, 'fpl_history.csv'),
                   index=False, encoding='utf-8')


if __name__ == '__main__':
    fetch_and_save_history()
