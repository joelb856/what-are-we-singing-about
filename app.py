import boto3
import json
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import dash_bootstrap_components as dbc
import re
from datetime import datetime, date, timedelta
import dash_bootstrap_components as dbc
from dash_bootstrap_templates import load_figure_template
from dash import Dash, dcc, html, Input, Output, callback, dash_table
from plotly.subplots import make_subplots

def plot_alltime_data(option):
    if (option == "Artist Popularity"):
        alltimefig = go.Figure()

        #Drop low-popularity artists to clean up plotting
        for series_name, series in df_artist_pop.items():
            if (series_name != 'date'):
                if (max(series) > 90) and (series.sum() > 2000):
                    alltimefig.add_trace(go.Scatter(x=df_artist_pop['date'], y=series, mode='lines+markers', name=series_name))
        alltimefig.update_layout(yaxis_title='Chart Popularity Score')
        alltimefig.add_annotation(text='Double click on an item in the legend to isolate it!', 
        align='right',
        showarrow=False,
        xref='paper',
        yref='paper',
        x=1.0,
        y=1.1,
        borderwidth=1)
    elif (option == "Emotions"):
        alltimefig = make_subplots(rows=2, cols=1, shared_xaxes=True)
        alltimefig.append_trace(go.Scatter(x=df_emotion['date'], y=np.zeros_like(df_emotion['positive']), mode='lines', visible=False, showlegend=False),1,1)
        alltimefig.append_trace(go.Scatter(x=df_emotion['date'], y=df_emotion['positive'] - df_emotion['negative'], mode='lines', fill='tonexty', name='', showlegend=False),1,1)
        for series_name, series in df_emotion.items():
            if (series_name != 'date') and (series_name != 'positive') and (series_name != 'negative'):
                alltimefig.append_trace(go.Scatter(x=df_emotion['date'], y=series, mode='lines', name=series_name),2,1)

        alltimefig.update_yaxes(title_text="Positive - Negative", row=1, col=1)
        alltimefig.update_yaxes(title_text="Affect Frequency", row=2, col=1)
        alltimefig.add_annotation(text='Double click on an item in the legend to isolate it!', 
                    align='right',
                    showarrow=False,
                    xref='paper',
                    yref='paper',
                    x=1.0,
                    y=1.1,
                    borderwidth=1)
    elif (option == "Word Frequency"):
        alltimefig = make_subplots()

        #Drop low-popularity artists to clean up plotting
        for series_name, series in df_word_freq.items():
            if (series_name != 'date'):
                if (max(series) > 0.004) or (series > 0.001).all():
                    alltimefig.add_trace(go.Scatter(x=df_word_freq['date'], y=series, mode='lines', name=series_name))
        alltimefig.update_layout(yaxis_title='Word Frequency')
        alltimefig.add_annotation(text='Double click on an item in the legend to isolate it!', 
            align='right',
            showarrow=False,
            xref='paper',
            yref='paper',
            x=1.0,
            y=1.1,
            borderwidth=1)
    elif (option == "Song Duration"):
        alltimefig = go.Figure([
        go.Scatter(
            x=df['date'],
            y=df['median_duration'],
            mode='lines',
            line=dict(color='rgb(31, 119, 180)'),
            name=''
        ),
        go.Scatter(
            x=df['date'],
            y=df['median_duration'] + df['err_duration_high'],
            mode='lines',
            marker=dict(color="#444"),
            line=dict(width=0),
            hoverinfo='skip'
        ),
        go.Scatter(
            x=df['date'],
            y=df['median_duration'] - df['err_duration_low'],
            marker=dict(color="#444"),
            line=dict(width=0),
            mode='lines',
            fillcolor='rgba(31, 119, 180, 0.4)',
            fill='tonexty',
            hoverinfo = 'skip'
        )
        ])

        alltimefig.update_layout(
            yaxis_title='Average Song Duration (minutes)',
            showlegend=False
        )
    elif (option == "Weeks on Chart"):
        alltimefig = go.Figure([
        go.Scatter(
            x=df['date'],
            y=df['median_weeks'],
            mode='lines',
            line=dict(color='rgb(31, 119, 180)'),
            name=''
        ),
        go.Scatter(
            x=df['date'],
            y=df['median_weeks'] + df['err_weeks_high'],
            mode='lines',
            marker=dict(color="#444"),
            line=dict(width=0),
            hoverinfo='skip'
        ),
        go.Scatter(
            x=df['date'],
            y=df['median_weeks'] - df['err_weeks_low'],
            marker=dict(color="#444"),
            line=dict(width=0),
            mode='lines',
            fillcolor='rgba(31, 119, 180, 0.4)',
            fill='tonexty',
            hoverinfo = 'skip'
        )
        ])

        alltimefig.update_layout(
            yaxis_title='Average Weeks Spent on Chart',
            showlegend=False
        )
    else:
        pass

    return alltimefig

def plot_weekly_data(option, songrange, date_selection):
    datetime_selection = datetime.combine(date_selection, datetime.min.time())
    date_closest_before = str(min([i for i in df['date'] if i <= datetime_selection], key=lambda x: abs(x - datetime_selection)).date())
    song_series = pd.Series(df['artist'][date_closest_before]) + ' - ' + pd.Series(df['song'][date_closest_before])
    if (option == "Song Duration"):
        weeklyfig = px.bar(x=song_series, y=pd.Series(df['duration'][date_closest_before]).astype('float')/60000.)
        weeklyfig.update_layout(yaxis_title="Song Duration (minutes)")
    elif (option == "Weeks on Chart"):
        weeklyfig = px.bar(x=song_series, y=df['weeks_on_chart'][date_closest_before])
        weeklyfig.update_layout(yaxis_title="Weeks on Chart")
    else:
        pass

    songrange[0] -= 0.5
    songrange[1] -= 0.5
    weeklyfig.update_xaxes(range=songrange)
    weeklyfig.update_layout(xaxis_title="")

    return weeklyfig

alltime_options = ["Artist Popularity", "Emotions", "Word Frequency", "Song Duration", "Weeks on Chart"]
weekly_options = ["Song Duration", "Weeks on Chart"]

app = Dash(__name__, external_stylesheets=[dbc.themes.CYBORG])
load_figure_template("cyborg")
server = app.server
s3 = boto3.resource('s3', region_name='us-east-2')
df = pd.read_json(s3.Object('what-are-we-singing-about', 'df_final.json').get()['Body'].read().decode('UTF-8'))
df_artist_pop = pd.read_json(s3.Object('what-are-we-singing-about', 'df_artist_pop.json').get()['Body'].read().decode('UTF-8'))
df_emotion = pd.read_json(s3.Object('what-are-we-singing-about', 'df_emotion.json').get()['Body'].read().decode('UTF-8'))
df_word_freq = pd.read_json(s3.Object('what-are-we-singing-about', 'df_word_freq.json').get()['Body'].read().decode('UTF-8'))

alltimefig = plot_alltime_data("Artist Popularity")
weeklyfig = plot_weekly_data("Weeks on Chart", [0,25], date.today())

app.layout = html.Div(children=[dbc.Row(html.H1('What are we Singing About?'),
                                        style={'textAlign': 'center'
                                               }),
                                dbc.Row(html.P("Lyric and metadata analysis of Billboard Hot 100 Songs, updated every week."),
                                        style={'textAlign':'center'
                                               }),
                                dbc.Row([dbc.Col([html.H3("Trends"),
                                                  dbc.Row(dcc.Dropdown(alltime_options,
                                                                       value='Artist Popularity', id='alltime-option-dropdown'),
                                                                       style={'margin-bottom':'15px', 'margin-top':'15px'}),], width=2
                                                 ),
                                         dbc.Col(dcc.Graph(figure=alltimefig, id='alltime-plot'),
                                                 style = {'margin-left':'15px', 'margin-top':'7px', 'margin-right':'15px', 'margin-bottom':'75px'})]),
                                dbc.Row([dbc.Col([html.H3("Weekly Rankings"), dcc.Dropdown(weekly_options, value='Weeks on Chart', id='weekly-option-dropdown'), dcc.DatePickerSingle(calendar_orientation='vertical', placeholder='Select a date', date=date.today(), min_date_allowed=min(df['date']), max_date_allowed=date.today(), id='date-picker'
                                                                                                                                                                                      )], width=2),
                                         dbc.Col([dcc.RangeSlider(min=0, max=100, step=5, value=[0, 20], id='range-slider'), dcc.Graph(figure=weeklyfig, id='weekly-plot'
                                                                                                                                       )])]),
                                dbc.Row(html.A("For more info, check out out the documentation on GitHub!", href='https://github.com/joelb856/what-are-we-singing-about', target="_blank"), style={'textAlign':'center'
                                                                                                                                                                                                   })],
                                style = {'margin-left':'50px', 'margin-right':'50px'})

@callback(
    Output('alltime-plot', 'figure'),
    Input('alltime-option-dropdown', 'value')
)

def update_alltime_graph(alltime_option):
    alltimefig = plot_alltime_data(alltime_option)

    return alltimefig

@callback(
    Output('weekly-plot', 'figure'),
    Input('weekly-option-dropdown', 'value'),
    Input('range-slider', 'value'),
    Input('date-picker', 'date')
)

def update_weekly_graph(weekly_option, songrange, date_selection):
    try:
        date_formatted = datetime.strptime(date_selection, '%Y-%m-%d').date()
        weeklyfig = plot_weekly_data(weekly_option, songrange, date_formatted)
    except:
        weeklyfig = plot_weekly_data(weekly_option, songrange, date.today())

    return weeklyfig

if __name__ == "__main__":
    app.run_server(debug=True)