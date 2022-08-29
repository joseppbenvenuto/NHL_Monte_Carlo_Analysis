import dash
import dash_bootstrap_components as dbc
import dash_mantine_components as dmc
from dash import dcc, html, Input, Output
import dash_auth

import numpy as np
import pandas as pd
import random as rnd

import plotly.graph_objs as go


######################################################################################################################
# NEW BLOCK - Pre app setup
######################################################################################################################

# Set login credentials
USERNAME_PASSWORD_PAIRS = [['data','analyst']]

# Establish app
app = dash.Dash(
    __name__,
    external_stylesheets = [dbc.themes.BOOTSTRAP]
    )

# Set login credentials
auth = dash_auth.BasicAuth(app,USERNAME_PASSWORD_PAIRS)
server = app.server

nhl = pd.read_csv('Data/Monte_Carlo_Data.csv')

team_list = nhl['Team'].unique()
team_list.sort()
team_options = [{'label': team, 'value': team} for team in team_list]


######################################################################################################################
# NEW BLOCK - App layout
######################################################################################################################


# Set app layout
app.layout = html.Div([
    
    # Header
    html.Div([
        
        html.H1(
            'NHL Game Simulations',
             style = {
                 'padding':10,
                 'margin':0,
                 'font-family':'Arial, Helvetica, sans-serif',
                 'background':'#00008B',
                 'color':'#FFFFFF',
                 'textAlign':'center'
            }
        )
    ]),
    
    # Set average score for simulation
    html.Div([
        
        # Header
        dbc.Row(
            
            html.H2(
                'Average Score', 
                style = {
                    'textAlign':'left',
                    'font-family':'Arial, Helvetica, sans-serif',
                    'padding-top':10,
                    'margin-left': 30
                }
            )
        ),
        
        # Score 1
        dbc.Row(
            
            html.Div(
                id = 's1', 
                style = {
                    'textAlign':'left',
                    'font-family':'Arial, Helvetica, sans-serif',
                    'padding-top':10,
                    'margin-left': 30
                }
            )
        ),
        
        # Score 2
        dbc.Row(
            
            html.Div(
                id = 's2', 
                style = {
                    'textAlign':'left',
                    'font-family':'Arial, Helvetica, sans-serif',
                    'padding-bottom':5,
                    'margin-left': 30
                }
            )
        )
    ], style = {'padding-top': 10}),
    
    # Dropdowns
    html.Div([
        
        dbc.Row([
            
            # Team 1
            dbc.Col([
                
                html.H2('Team1'),
                
                dcc.Dropdown(
                    id = 'team1',
                    options = team_options,
                    value = 'Toronto Maple Leafs'
                    ),
                
                html.Div(
                    id = 'teamOne',
                    style = {'padding-top':15}
                )
            ]),
            
            # Team 2
            dbc.Col([
                
                html.H2('Team2'),
                
                dcc.Dropdown(
                    id = 'team2',
                    options = team_options,
                    value ='Dallas Stars'
                ),
                
                html.Div(
                    id = 'teamTwo',
                    style = {'padding-top':15}
                )
            ])
        ])
    ], 
        style = {
            'font-family':'Arial, Helvetica, sans-serif',
            'padding-top':10,
            'padding-right':'5%',
            'padding-left':'5%',
            'textAlign':'center'
        }
    ),
    
    # Bar chart
    html.Div([
    
        dcc.Graph(id = 'feature_graphic')
        
    ],
        style = {
            'font-family':'Arial, Helvetica, sans-serif',
            'padding-bottom': 20
        }
    ),
    
    html.Div([
        
        html.H1(
            'Instructions',
            style = {
                'padding':10,
                'margin':0,
                'font-family':'Arial, Helvetica, sans-serif',
                'background':'#00008B',
                'color':'#FFFFFF',
                'textAlign':'center'
            }
        )
    ]),
    
    # Instrcutions
    html.Div(
        "The dashboard displays the probability of winning between two selected teams. Each \
        dropdown list, for headings Team1 and Team2, shows a list of all the teams in the NHL and \
        allows the user to select the two teams they want to face off. Once the teams are chosen, the \
        probabilities for each team to win will be generated. The probabilities can be viewed under the \
        dropdown lists and through the bar plot. The bar representing the team with the higher probability \
        of winning will be coloured green, and the bar representing the team with the lower probability of \
        winning will be coloured red. The average score for the matchup can be found under the average score \
        heading in the top left corner.",
        
        style = {
            'padding-top':60,
            'padding-right':20,
            'padding-left':20,
            'line-height':30,
            'padding-bottom':60,
            'fontSize':20,
            'textAlign':'center',
            'font-family':'Arial, Helvetica, sans-serif'
        }
    ),
    
])


######################################################################################################################
# NEW BLOCK - App layout
######################################################################################################################

@app.callback(
    Output('feature_graphic','figure'),
    Output('teamOne','children'),
    Output('teamTwo','children'),
    Output('s1','children'),
    Output('s2','children'),
    Input('team1','value'),
    Input('team2','value')
)


def update_graph(team1, team2):
    
    # get data
    nhl_mc = nhl.copy()

    # Drop columns
    nhl_mc = nhl_mc.drop(
        [
            'sk_games',
            'Date',
            'Opponent'
        ], 
        axis = 1,
        errors = 'ignore'
    )

    # Split the data basis teams
    data1 = nhl_mc.iloc[(nhl_mc['Team'] == team1).values, [0,1,2]] 
    data2 = nhl_mc.iloc[(nhl_mc['Team'] == team2).values, [0,1,2]] 

    # Goals for stats
    team1_mean_pts = data1['Real_Score'].mean()
    team2_mean_pts = data2['Real_Score'].mean()
    team1_SD_pts = data1['Real_Score'].std()
    team2_SD_pts = data2['Real_Score'].std()

    # Goals against stats
    team1_mean_pts_a = data1['Opponent_Real_Score'].mean()
    team2_mean_pts_a = data2['Opponent_Real_Score'].mean()
    team1_SD_pts_a = data1['Opponent_Real_Score'].std()
    team2_SD_pts_a = data2['Opponent_Real_Score'].std()


    # 1 game simulation
    def sim():
        # Team1: randome points for + opponents points against
        team1_score = (rnd.gauss(team1_mean_pts,team1_SD_pts) + rnd.gauss(team2_mean_pts_a,team2_SD_pts_a)) / 2
        team2_score = (rnd.gauss(team2_mean_pts,team2_SD_pts) + rnd.gauss(team1_mean_pts_a,team1_SD_pts_a)) / 2

        # Decide game outcome
        if team1_score > team2_score:
            return (1, team1_score, team2_score)
        elif team1_score < team2_score:
            return (-1, team1_score, team2_score)
        else:
            return (0, team1_score, team2_score)


    # Simulate 100,000 games
    def games_sim():
        # Outcomes per team
        team1_win = 0
        team2_win = 0
        tie = 0

        # Scores
        score1 = []
        score2 = []

        # Simulate games
        for i in range (100000):
            # Sim game
            game_sim = sim()
            game_sim1 = game_sim[0]
            sc1 = game_sim[1]
            sc2 = game_sim[2]

            # Append scores to list
            score1.append(sc1)
            score2.append(sc2)

            # Count outcomes
            if game_sim1 == 1:
                team1_win += 1
            elif game_sim1 == -1:
                team2_win += 1
            else:
                tie += 1

        # Get proportion of outcomes
        team1_p = team1_win / 100000
        team2_p = team2_win / 100000
        tie_p = tie / 100000

        # Get average scores
        score1_p = sum(score1) / 100000
        score2_p = sum(score2) / 100000

        return (team1_p, team2_p, tie_p, score1_p, score2_p)
    
    # Format game sim data
    game_sims = games_sim()

    sd = [
        {'team1': team1,'prob_to_win1':  round(game_sims[0] * 100, 0)},
        {'team2': team2,'prob_to_win2': round(game_sims[1] * 100, 0)},
        {'tie':'Tie', 'prob_to_tie': round(game_sims[2],2)},
        {'team1_s': team1, 'prob_score1': round(game_sims[3],2)},
        {'team2_s': team2, 'prob_score2': round(game_sims[4],2)}
    ]

    game_sims_df = pd.DataFrame(sd)
    
    # Teams win probabilities and scores
    team1_p = game_sims_df.iloc[0,1]
    team2_p = game_sims_df.iloc[1,3]

    team1_p_string = str(int(game_sims_df.iloc[0,1]))
    team2_p_string  = str(int(game_sims_df.iloc[1,3]))
    team1_s_string = str(float(game_sims_df.iloc[3,7]))
    team2_s_string = str(float(game_sims_df.iloc[4,9]))
    
    # conditional formatting per outcome
    if team1_p > team2_p:
        t1 = '#008000'
        t2 = '#B22222'
    if team1_p < team2_p:
        t1 = '#B22222'
        t2 = '#008000'
    if team1_p == team2_p:
        t1 = '#008000'
        t2 = '#008000'


    data = [
        go.Bar(
            x = game_sims_df['team1'],
            y = game_sims_df['prob_to_win1'],
            marker = {'color':t1},
            width = 0.3,
            showlegend = False,
            name = team1
        ),
        go.Bar(
            x = game_sims_df['team2'],
            y = game_sims_df['prob_to_win2'],
            marker = {'color':t2},
            width = 0.3,
            showlegend = False,
            name = team2
        )
    ]

    t1p = '{} have a {}% chance to win'.format(team1, team1_p_string)
    t2p = '{} have a {}% chance to win'.format(team2, team2_p_string)
    t1s = '{}: {}'.format(team1, team1_s_string)
    t2s = '{}: {}'.format(team2, team2_s_string)
    
    data = {
        'data': data,
        'layout':go.Layout(
            yaxis = {
                'title':'Probabilities',
                'showgrid': False,
            },
            font = {'color': '#111111'}
        )
    }

    return data , t1p, t2p, t1s, t2s

if __name__ == '__main__':
    app.run_server()

    