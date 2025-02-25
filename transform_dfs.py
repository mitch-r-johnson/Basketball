
#Remove opponent box score rows from df
def drop_other_teams(df: object, team_id: int) -> object:
    for index, row in df.iterrows():
        if df.loc[index, 'team_id'] != team_id:
            df.drop(index,inplace=True)
    return df


#Create a column with win or loss per box score
def win_or_loss(df: object, team_id: int) -> object:
    for index, row in df.iterrows():
        if df.loc[index, 'teams_home_id'] == team_id:
            if df.loc[index, 'scores_home_total'] > df.loc[index, 'scores_away_total']:
                df.loc[index, 'W/L'] = 1
            else:
                df.loc[index, 'W/L'] = 0
        elif df.loc[index, 'teams_away_id'] == team_id:
            if df.loc[index, 'scores_home_total'] < df.loc[index, 'scores_away_total']:
                df.loc[index, 'W/L'] = 1
            else:
                df.loc[index, 'W/L'] = 0
    return df

#Create a column with points total for intended team
def score(df: object, team_id: int) -> object:
    for index, row in df.iterrows():
        if df.loc[index, 'teams_home_id'] == team_id:
            df.loc[index, 'points'] = df.loc[index, 'scores_home_total']
        else:
            df.loc[index, 'points'] = df.loc[index, 'scores_away_total']
    return df


