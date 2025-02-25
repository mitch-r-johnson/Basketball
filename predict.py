import model

#Dates of the current NBA season
current_season_df = model.concat_predictions("2024-10-22","2025-02-24")
print(current_season_df)

current_season_df[current_season_df['winners'] == current_season_df['predictions']]['game_ids'].count()

prediction_accuracy = current_season_df[current_season_df['winners'] == current_season_df['predictions']]['game_ids'].count()/current_season_df['game_ids'].count()
print(f"Prediction accuracy: {prediction_accuracy}")