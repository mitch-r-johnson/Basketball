import get_data
import transform_dfs
import pandas as pd

def main(team_id: str) -> object:
    games = get_data.transform_game(get_data.get_5yr_games(team_id))
    games_df = pd.DataFrame(games)

    box_scores = get_data.flatten_games_by_game_id(get_data.get_5yr_box_scores(team_id))
    box_scores_df = pd.DataFrame(box_scores)

    games_df.rename(columns={'id': 'game_id'}, inplace=True)
    games_box_scores_merged_df = pd.merge(games_df, box_scores_df, on='game_id', how='outer')

    team_id_df = transform_dfs.drop_other_teams(games_box_scores_merged_df,team_id)
    team_id_df.set_index("game_id")
    columns_to_drop = ["time", "timestamp", "timezone", "stage", "week", "venue", "status_long",
                       "status_short", "status_timer", "league_id", "league_name", "league_type", "league_logo",
                       "country_id", "country_name", "country_code", "country_flag", "teams_home_logo",
                       "teams_away_logo", "scores_home_quarter_1", "scores_home_quarter_2", "scores_home_quarter_3",
                       "scores_home_quarter_4", "scores_home_over_time", "scores_away_quarter_1",
                       "scores_away_quarter_2", "scores_away_quarter_3", "scores_away_quarter_4",
                       "scores_away_over_time", "field_goals_attempts", "threepoint_goals_attempts",
                       "freethrows_goals_attempts"]
    team_id_df_filtered = team_id_df.drop(columns_to_drop, axis=1)

    WL_points_df = transform_dfs.score(transform_dfs.win_or_loss(team_id_df_filtered, team_id), team_id)
    clean_df = WL_points_df.dropna()

    clean_df.to_csv(f"/Users/mitchjohnson/PycharmProjects/Basketball/Data/{team_id}.csv")

for team_id in range(132,162):
    main(team_id)