import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
import get_data
import numpy as np
import transform_dfs
from datetime import date

today = date.today()
formatted_date = today.strftime("%Y-%m-%d")

def matchups(data):
    matchup_dict = {}

    for game in data.get('response', []):
        game_id = game['id']
        home_team_id = game['teams']['home']['id']
        away_team_id = game['teams']['away']['id']

        matchup_dict[game_id] = [home_team_id, away_team_id]

    return matchup_dict

def recent_three_box_scores(team_id):
    df = pd.read_csv(f"/Users/mitchjohnson/PycharmProjects/Basketball/Data/{team_id}.csv")
    columns_to_drop = ['Unnamed: 0','game_id','teams_home_id','teams_away_id','date','league_season','teams_home_name','teams_away_name','team_id','W/L']
    model_data_df = df.drop(columns_to_drop,axis=1)
    recent_three_df = model_data_df.iloc[-3:]
    recent_three_averages = recent_three_df.mean(numeric_only=True)
    recent_three_averages_df = pd.DataFrame(recent_three_averages).T
    return recent_three_averages_df

def make_prediction(team_id,box_score):
    df = pd.read_csv(f"/Users/mitchjohnson/PycharmProjects/Basketball/Data/{team_id}.csv")
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

def pick_winners(matchups):
    prediction_dict = {}
    for game,teams in matchups.items():
        if make_prediction(teams[0],recent_three_box_scores(teams[0])) > make_prediction(teams[1],recent_three_box_scores(teams[1])):
            prediction_dict.update({game:teams[0]})
        else:
            prediction_dict.update({game:teams[1]})
    return prediction_dict

#Calling it on a historical day
todays_games = get_data.get_dates_games("2024-2025","2025-02-12")
matchups = matchups(todays_games)
# matchups_dated = {formatted_date:matchups}
# matchups_df = pd.DataFrame(matchups_dated)
# running_matchups_list = pd.read_csv("/Users/mitchjohnson/PycharmProjects/Basketball/Data/matchups.csv")
# updated_df = pd.concat([running_matchups_list,matchups_df],ignore_index=True)
# updated_df.to_csv("/Users/mitchjohnson/PycharmProjects/Basketball/Data/matchups.csv")
print(matchups)

picks = pick_winners(matchups)
# picks_dated = {formatted_date:picks}
# picks_df = pd.DataFrame(picks_dated)
# picks_df.to_csv("/Users/mitchjohnson/PycharmProjects/Basketball/Data/picks.csv")
print(picks)
