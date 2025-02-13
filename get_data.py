#%%
import httpx
import pandas as pd
#%%
def call_api(params: str) -> object:
    headers = {
        'x-rapidapi-host': "v1.basketball.api-sports.io",
        'x-rapidapi-key': "db8819fa204d3f23a50ba996ff64ae83"
    }
    url = "http://v1.basketball.api-sports.io"
    response = httpx.get(f"{url}{params}", headers=headers)
    nested_json = response.json()
    return nested_json
#%%
def get_team(team_id: str) -> object:
    team = call_api(f"/teams?id={team_id}&league=12&season=2024-2025")
    return team

def get_season_stats(team_id: str) -> object:
    season_stats = call_api(f"/statistics?season=2024-2025&team={team_id}&league=12")
    return season_stats

def get_games(season: str, team_id: str) -> object:
    game = call_api(f"/games?league=12&season={season}&team={team_id}")
    return game

def get_game_ids(season: str, team_id: str) -> object:
    historical_games = call_api(f"/games?league=12&season={season}&team={team_id}")
    game_ids = []
    for item in historical_games["response"]:
        game_ids.append(item["id"])
    return game_ids

def get_box_scores(game_ids: str) -> object:
    box_scores = []
    for game_id in game_ids:
        box_scores.append(call_api(f"/games/statistics/teams?id={game_id}"))
    return box_scores

def get_5yr_box_scores(team_id: str) -> object:
    five_years = []
    seasons = ["2024-2025","2023-2024","2022-2023","2021-2022","2020-2021"]
    for season in seasons:
        five_years.extend([get_box_scores(get_game_ids(season,team_id))])
    return five_years

def get_5yr_games(team_id: str) -> object:
    five_years = []
    seasons = ["2024-2025","2023-2024","2022-2023","2021-2022","2020-2021"]
    for season in seasons:
        five_years.extend([get_games(season,team_id)])
    return five_years
#%%
def transform_game(data_list: object) -> object:
    """
    Extracts, flattens, and indexes all data under the 'response' key by 'id'.

    Parameters:
    data_list (list): A list of dictionaries containing a 'response' key.

    Returns:
    list: A fully flattened list of dictionaries, indexed by 'id'.
    """
    flattened_data = []

    for data in data_list:
        if 'response' in data and isinstance(data['response'], list):
            for item in data['response']:
                if isinstance(item, dict):
                    # Flatten the dictionary
                    flat_item = flatten_dict(item)

                    # Ensure it has an 'id' key for indexing
                    if 'id' in flat_item:
                        flattened_data.append(flat_item)
                    else:
                        print(f"Warning: Skipping entry without 'id' - {flat_item}")

    return flattened_data

import pandas as pd

def flatten_dict(d: object, parent_key = '', sep = '_') -> object:
    """
    Recursively flattens a nested dictionary.

    Parameters:
    d (dict): The dictionary to flatten.
    parent_key (str): The prefix for nested keys (used for recursion).
    sep (str): Separator for nested keys.

    Returns:
    dict: A flattened dictionary with concatenated keys.
    """
    flattened = {}
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            flattened.update(flatten_dict(v, new_key, sep=sep))
        else:
            flattened[new_key] = v
    return flattened

def flatten_games_by_game_id(nested_json_data: object) -> object:
    """
    Extracts and flattens all game response data to the 'game_id' level,
    keeping all associated team data while removing unnecessary metadata.

    Parameters:
    nested_json_data (list): List containing nested game records.

    Returns:
    list: A fully flattened list of dictionaries, indexed by 'game_id'.
    """

    # Flatten the outermost list layer
    json_data = [item for sublist in nested_json_data for item in sublist] if isinstance(nested_json_data, list) else nested_json_data

    flattened_data = []

    for entry in json_data:
        if 'response' in entry and isinstance(entry['response'], list):
            for game in entry['response']:
                if isinstance(game, dict):
                    # Extract game_id and team_id
                    game_id = game.get('game', {}).get('id')
                    team_id = game.get('team', {}).get('id')

                    if game_id is not None and team_id is not None:
                        # Flatten the game dictionary
                        flat_game = flatten_dict(game)

                        # Remove redundant 'game_id' and 'team_id' fields
                        flat_game.pop('game_id', None)
                        flat_game.pop('team_id', None)

                        # Include game_id and team_id at the top level
                        flat_game['game_id'] = game_id
                        flat_game['team_id'] = team_id

                        # Append flattened game data
                        flattened_data.append(flat_game)

    return flattened_data

def transform_team(json_data: object) -> object:
    response = json_data.get("response", [])

    transformed_data = []

    for team in response:
        country = team.get("country", {})

        transformed_data.append({
            "team_id": team.get("id"),
            "team_name": team.get("name"),
            "team_logo": team.get("logo"),
            "is_national": team.get("nationnal"),
            "country_id": country.get("id"),
            "country_name": country.get("name"),
            "country_code": country.get("code"),
            "country_flag": country.get("flag"),
        })

    return transformed_data

def transform_season_stats(json_data: object) -> object:
    response = json_data.get("response", {})

    transformed_data = []

    # Extracting structured data
    country = response.get("country", {})
    league = response.get("league", {})
    team = response.get("team", {})
    games = response.get("games", {})
    points = response.get("points", {})

    # Creating a list of dictionaries with relevant information
    transformed_data.append({
        "country_id": country.get("id"),
        "country_name": country.get("name"),
        "country_code": country.get("code"),
        "country_flag": country.get("flag"),
        "league_id": league.get("id"),
        "league_name": league.get("name"),
        "league_type": league.get("type"),
        "league_season": league.get("season"),
        "league_logo": league.get("logo"),
        "team_id": team.get("id"),
        "team_name": team.get("name"),
        "team_logo": team.get("logo"),
        "games_played_home": games.get("played", {}).get("home"),
        "games_played_away": games.get("played", {}).get("away"),
        "games_played_all": games.get("played", {}).get("all"),
        "wins_home": games.get("wins", {}).get("home", {}).get("total"),
        "wins_away": games.get("wins", {}).get("away", {}).get("total"),
        "wins_all": games.get("wins", {}).get("all", {}).get("total"),
        "loses_home": games.get("loses", {}).get("home", {}).get("total"),
        "loses_away": games.get("loses", {}).get("away", {}).get("total"),
        "loses_all": games.get("loses", {}).get("all", {}).get("total"),
        "points_for_home": points.get("for", {}).get("total", {}).get("home"),
        "points_for_away": points.get("for", {}).get("total", {}).get("away"),
        "points_for_all": points.get("for", {}).get("total", {}).get("all"),
        "points_against_home": points.get("against", {}).get("total", {}).get("home"),
        "points_against_away": points.get("against", {}).get("total", {}).get("away"),
        "points_against_all": points.get("against", {}).get("total", {}).get("all"),
    })

    return transformed_data
