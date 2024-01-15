import os
import requests
import json
import boto3
from datetime import datetime

from secrets import LASTFM_API_KEY

HOT_100_URL = 'https://raw.githubusercontent.com/mhollingshead/billboard-hot-100/main/recent.json'
LASTFM_API_BASE = 'http://ws.audioscrobbler.com/2.0/'
LYRIST_API_BASE = 'https://lyrist.vercel.app/api/'

def clear_terminal():
    os.system('cls' if os.name == 'nt' else 'clear')

def grab_data():
    response = requests.get(HOT_100_URL)
    hot_100 = response.json()
    date = hot_100['date']

    #For each entry, attempt to grab metadata from last.fm and lyrics from lyrist
    count = 1
    for entry in hot_100['data']:

        clear_terminal()
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

        response = requests.get(LYRIST_API_BASE + f"/{entry['artist'].replace(' ', '+')}/{entry['song'].replace(' ', '+')}")
        lyrics = response.json()
        if 'lyrics' in lyrics:
            entry['lyrics'] = response.json()['lyrics']

    #Write locally
    outfile = f"data/hot-100-{hot_100['date']}.json"
    with open(outfile, 'w+') as f:
        json.dump(hot_100, f)

    #Then upload to s3
    s3 = boto3.resource('s3')
    with open(outfile, 'rb') as f:
        response = s3.Bucket('what-are-we-singing-about').put_object(Key=outfile, Body=f)

    return response
    
if __name__ == '__main__':
    response = grab_data()
    print(response)