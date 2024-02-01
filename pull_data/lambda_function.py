import json
import requests
import boto3

from secret_stuff import LASTFM_API_KEY

HOT_100_URL = 'https://raw.githubusercontent.com/mhollingshead/billboard-hot-100/main/recent.json'
HOT_100_HISTORIC_BASE = 'https://raw.githubusercontent.com/mhollingshead/billboard-hot-100/main/date/'
LASTFM_API_BASE = 'http://ws.audioscrobbler.com/2.0/'
LYRIST_API_BASE = 'https://lyrist.vercel.app/api/'

def pull_data(**kwargs):

    #If date_string in kwargs, use that date. If not
    date_string = kwargs.get('date_string', None)
    if date_string:
        url = HOT_100_HISTORIC_BASE + date_string + '.json'
    else:
        url = HOT_100_URL

    response = requests.get(url)
    hot_100 = response.json()
    date = hot_100['date']

    #For each entry, attempt to grab metadata from last.fm and lyrics from lyrist
    count = 1
    for entry in hot_100['data']:

        print(f"Gathering data for song {count} - {entry['song']} by {entry['artist']}")
        count += 1

        response = requests.get(LASTFM_API_BASE + f"?method=track.getInfo&api_key={LASTFM_API_KEY}&artist={entry['artist']}&track={entry['song']}&format=json")
        trackInfo = response.json()
        if 'error' in trackInfo:
            pass
        else:
            entry['duration'] = trackInfo['track']['duration']
            entry['lastfm_listeners'] = trackInfo['track']['listeners']
            entry['lastfm_playcount'] = trackInfo['track']['playcount']
            entry['toptags'] = trackInfo['track']['toptags']
            if 'wiki' in trackInfo['track']:
                entry['summary'] = trackInfo['track']['wiki']['summary']

        try:
            response = requests.get(LYRIST_API_BASE + f"/{entry['artist'].replace(' ', '+')}/{entry['song'].replace(' ', '+')}")
            response.raise_for_status()
            lyrics = response.json()
            if 'lyrics' in lyrics:
                entry['lyrics'] = response.json()['lyrics']
        except:
            pass

    #Upload JSON data to s3
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket_name='what-are-we-singing-about', key=f"data/hot-100-{hot_100['date']}.json")
    obj.put(Body=bytes(json.dumps(hot_100).encode('UTF-8')))

def lambda_handler(event, context):
    pull_data()