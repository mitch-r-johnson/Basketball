#%%
import httpx
import pandas as pd
from datetime import date
#%%
today = date.today()
formatted_date = today.strftime("%Y-%m-%d")

def call_api(params: str) -> object:
    headers = {
        'x-rapidapi-host': "v1.basketball.api-sports.io",
        'x-rapidapi-key': "db8819fa204d3f23a50ba996ff64ae83"
    }
    url = "http://v1.basketball.api-sports.io"
    response = httpx.get(f"{url}{params}", headers=headers)
    nested_json = response.json()
    return nested_json

def get_team(team_id: str) -> object:
    team = call_api(f"/teams?id={team_id}&league=12&season=2024-2025")
    return team

def get_season_stats(team_id: str) -> object:
    season_stats = call_api(f"/statistics?season=2024-2025&team={team_id}&league=12")
    return season_stats

def get_games(season: str, team_id: str) -> object:
    game = call_api(f"/games?league=12&season={season}&team={team_id}")
    return game

def get_dates_games(season: str, date=formatted_date) -> object:
    todays_game = call_api(f"/games?date={date}&season={season}&league=12")
    return todays_game

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
    seasons = ["2023-2024","2022-2023","2021-2022","2020-2021","2019-2020"]
    for season in seasons:
        five_years.extend([get_box_scores(get_game_ids(season,team_id))])
    return five_years

def get_5yr_games(team_id: str) -> object:
    five_years = []
    seasons = ["2023-2024","2022-2023","2021-2022","2020-2021","2019-2020"]
    for season in seasons:
        five_years.extend([get_games(season,team_id)])
    return five_years

def get_current_season_box_scores(team_id: str) -> object:
    current_season = get_box_scores(get_game_ids("2024-2025",team_id))
    return current_season

def get_current_season_games(team_id: str) -> object:
    current_season = get_games("2024-2025",team_id)
    return current_season
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

def transform_game2(data: object) -> object:
    """
    Extracts, flattens, and indexes all game data from the JSON response.

    The JSON is expected to have the following structure:
      {
          'get': 'games',
          'parameters': { ... },
          'errors': [],
          'results': <number>,
          'response': [ {game_record1}, {game_record2}, ... ]
      }

    Each game record is a dictionary that already contains an 'id' key.

    Returns:
      list: A fully flattened list of game dictionaries.
    """
    flattened_data = []

    # Determine the list of game records
    if isinstance(data, dict) and 'response' in data:
        game_list = data['response']
    elif isinstance(data, list):
        # In case a list of dictionaries was passed in
        game_list = []
        for item in data:
            if isinstance(item, dict) and 'response' in item:
                game_list.extend(item['response'])
    else:
        print("Warning: Input data is not in the expected format.")
        return flattened_data

    # Process each game record
    for item in game_list:
        if isinstance(item, dict):
            # Flatten the dictionary using flatten_dict (assumed defined elsewhere)
            flat_item = flatten_dict(item)
            if 'id' in flat_item:
                flattened_data.append(flat_item)
            else:
                print(f"Warning: Skipping entry without 'id' - {flat_item}")
        else:
            print(f"Warning: Expected a dictionary in 'response', got {type(item)}. Skipping.")

    return flattened_data

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

def flatten_games_by_game_id2(nested_json_data: object) -> object:
    """
    Extracts and flattens all game response data to the 'game_id' level,
    keeping all associated team data while removing unnecessary metadata.

    Parameters:
      nested_json_data (list): List containing nested game records.

    Returns:
      list: A fully flattened list of dictionaries, indexed by 'game_id'.
    """
    # Flatten the outermost list layer if necessary.
    if isinstance(nested_json_data, list):
        json_data = []
        for sublist in nested_json_data:
            if isinstance(sublist, list):
                json_data.extend(sublist)
            else:
                json_data.append(sublist)
    else:
        json_data = [nested_json_data]

    flattened_data = []

    for entry in json_data:
        if not isinstance(entry, dict):
            print(f"Warning: Expected dict but got {type(entry)}. Skipping entry: {entry}")
            continue

        if 'response' in entry and isinstance(entry['response'], list):
            for game in entry['response']:
                if isinstance(game, dict):
                    # Extract game_id and team_id from the nested structure.
                    game_id = game.get('game', {}).get('id')
                    team_id = game.get('team', {}).get('id')

                    if game_id is not None and team_id is not None:
                        # Flatten the game dictionary (assuming flatten_dict is defined)
                        flat_game = flatten_dict(game)

                        # Remove redundant keys if present.
                        flat_game.pop('game_id', None)
                        flat_game.pop('team_id', None)

                        # Insert game_id and team_id at the top level.
                        flat_game['game_id'] = game_id
                        flat_game['team_id'] = team_id

                        flattened_data.append(flat_game)
                    else:
                        print(f"Warning: Missing game_id or team_id in game: {game}")
                else:
                    print(f"Warning: Expected dict in 'response' list but got {type(game)}. Skipping game entry: {game}")
        else:
            print(f"Warning: 'response' key missing or not a list in entry: {entry}")

    return flattened_data

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

def flatten_json2(data):
    flattened = []

    for game in data.get('response', []):
        flat_game = {
            'id': game['id'],
            'date': game['date'],
            'time': game['time'],
            'timestamp': game['timestamp'],
            'timezone': game['timezone'],
            'venue': game['venue'],
            'status_long': game['status']['long'],
            'status_short': game['status']['short'],
            'league_id': game['league']['id'],
            'league_name': game['league']['name'],
            'league_season': game['league']['season'],
            'league_logo': game['league']['logo'],
            'country_id': game['country']['id'],
            'country_name': game['country']['name'],
            'country_code': game['country']['code'],
            'country_flag': game['country']['flag'],
            'home_team_id': game['teams']['home']['id'],
            'home_team_name': game['teams']['home']['name'],
            'home_team_logo': game['teams']['home']['logo'],
            'away_team_id': game['teams']['away']['id'],
            'away_team_name': game['teams']['away']['name'],
            'away_team_logo': game['teams']['away']['logo'],
            'home_q1': game['scores']['home']['quarter_1'],
            'home_q2': game['scores']['home']['quarter_2'],
            'home_q3': game['scores']['home']['quarter_3'],
            'home_q4': game['scores']['home']['quarter_4'],
            'home_ot': game['scores']['home']['over_time'],
            'home_total': game['scores']['home']['total'],
            'away_q1': game['scores']['away']['quarter_1'],
            'away_q2': game['scores']['away']['quarter_2'],
            'away_q3': game['scores']['away']['quarter_3'],
            'away_q4': game['scores']['away']['quarter_4'],
            'away_ot': game['scores']['away']['over_time'],
            'away_total': game['scores']['away']['total'],
        }
        flattened.append(flat_game)

    return flattened
