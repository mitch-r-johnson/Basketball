import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
import get_data
import transform_dfs
from datetime import date, timedelta, datetime

today = date.today()
formatted_date = today.strftime("%Y-%m-%d")

teams = [i for i in range(132,162)]

def get_matchups(data):
    matchup_dict = {}

    for game in data.get('response', []):
        game_id = game['id']
        home_team_id = game['teams']['home']['id']
        away_team_id = game['teams']['away']['id']

        matchup_dict[game_id] = [home_team_id, away_team_id]

    return matchup_dict

def recent_four_box_scores(team_id,date):

    if team_id in teams:

        df = pd.read_csv(f"/Users/mitchjohnson/PycharmProjects/Basketball/Data/{team_id}-current.csv")
        columns_to_drop = ['Unnamed: 0','game_id','teams_home_id','teams_away_id','league_season','teams_home_name','teams_away_name','team_id','W/L']
        model_data_df = df.drop(columns_to_drop,axis=1)
        model_data_df['date'] = pd.to_datetime(model_data_df['date'], utc=True)
        given_date = pd.Timestamp(date, tz='UTC')
        recent_four_df = model_data_df[model_data_df['date'] <= given_date].sort_values(by='date', ascending=False).head(4)
        recent_four_drop_date = recent_four_df.drop('date',axis=1)
        recent_four_averages = recent_four_drop_date.mean(numeric_only=True)
        recent_four_averages_df = pd.DataFrame(recent_four_averages).T
        return recent_four_averages_df

def make_prediction(team_id,box_score):

    if team_id in teams:

        df = pd.read_csv(f"/Users/mitchjohnson/PycharmProjects/Basketball/Data/{team_id}-historical.csv")
        columns_to_drop = ['Unnamed: 0','game_id','teams_home_id','teams_away_id','date','league_season','teams_home_name','teams_away_name']
        model_data_df = df.drop(columns_to_drop,axis=1)
        X = model_data_df.drop(['W/L','team_id'],axis=1)
        y = model_data_df['W/L']
        X_train, X_test, y_train, y_test = train_test_split(X,y,test_size=.3)
        logmodel = LogisticRegression(max_iter=1200)
        logmodel.fit(X_train,y_train)
        predictions = logmodel.predict(box_score)
        probabilities = logmodel.predict_proba(box_score)
        return probabilities[0][1]

def pick_winners(matchups,date):
    prediction_dict = {}
    for game, teams in matchups.items():
        # Get predictions for both teams
        prediction1 = make_prediction(teams[0], recent_four_box_scores(teams[0],date))
        prediction2 = make_prediction(teams[1], recent_four_box_scores(teams[1],date))

        # Convert None values to 0 before comparison
        if prediction1 is None:
            prediction1 = 0
        if prediction2 is None:
            prediction2 = 0

        # Determine the winning team based on the predictions
        if prediction1 > prediction2:
            prediction_dict[game] = teams[0]
        else:
            prediction_dict[game] = teams[1]

    return prediction_dict

def make_df(date):
    todays_games = get_data.get_dates_games("2024-2025", date)
    matchups = get_matchups(todays_games)
    game_ids = [key for key in matchups]
    team1 = [v[0] for k,v in matchups.items()]
    team2 = [v[1] for k,v in matchups.items()]
    winners = get_winning_teams(todays_games)
    winners_lst = [v for k,v in winners.items()]
    predictions = pick_winners(matchups, date)
    predictions_lst = [v for k,v in predictions.items()]
    data = {"game_ids":game_ids, "team_1":team1,"team_2":team2,"winners":winners_lst,"predictions":predictions_lst}
    df = pd.DataFrame(data)
    return df

def concat_predictions(start_date, end_date):
    # Convert string dates to datetime objects
    formatted_start_date = datetime.strptime(start_date, "%Y-%m-%d")
    formatted_end_date = datetime.strptime(end_date, "%Y-%m-%d")

    df_lst = []

    current_date = formatted_start_date
    while current_date <= formatted_end_date:
        str_date = current_date.strftime("%Y-%m-%d")
        df_lst.append(make_df(str_date))
        current_date += timedelta(days=1)
    concat_df = pd.concat(df_lst)
    return concat_df

def get_unique_team_ids(games_data):
    """
    Extracts a sorted list of unique team IDs from the given games data.

    Parameters:
    games_data (dict): JSON-like dictionary containing game details.

    Returns:
    list: Sorted list of unique team IDs.
    """
    team_ids = set()

    for game in games_data.get('response', []):
        team_ids.add(game['teams']['home']['id'])
        team_ids.add(game['teams']['away']['id'])

    return sorted(team_ids)

# Example usage
games_json = {
    'get': 'games',
    'parameters': {'date': '2025-02-12', 'season': '2024-2025', 'league': '12'},
    'errors': [],
    'results': 4,
    'response': [
        {'id': 414637, 'teams': {'home': {'id': 154}, 'away': {'id': 159}}},
        {'id': 414638, 'teams': {'home': {'id': 143}, 'away': {'id': 151}}},
        {'id': 414639, 'teams': {'home': {'id': 136}, 'away': {'id': 140}}},
        {'id': 414640, 'teams': {'home': {'id': 155}, 'away': {'id': 146}}},
    ]
}

# Get unique team IDs
unique_team_ids = get_unique_team_ids(games_json)

def get_winning_teams(game_data):
    """
    Extracts the winning teams from the given game data, accounting for None values in scores.

    :param game_data: Dictionary containing game results.
    :return: Dictionary with game IDs as keys and winning team IDs as values.
    """
    winners = {}

    for game in game_data.get("response", []):
        game_id = game["id"]
        home_team_id = game["teams"]["home"]["id"]
        away_team_id = game["teams"]["away"]["id"]

        # Get the total scores; if None, convert to 0
        home_score = game["scores"]["home"].get("total")
        away_score = game["scores"]["away"].get("total")
        home_score = home_score if home_score is not None else 0
        away_score = away_score if away_score is not None else 0

        # Compare scores to determine the winning team
        winners[game_id] = home_team_id if home_score > away_score else away_team_id

    return winners