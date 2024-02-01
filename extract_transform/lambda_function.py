import boto3
import json
import re
import numpy as np
import pandas as pd
import io
from datetime import datetime
from collections import Counter
from nrclex import NRCLex
from langdetect import detect
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.probability import FreqDist
from nltk.stem import WordNetLemmatizer

STOP_WORDS = set(stopwords.words('english'))
BUCKET = 'what-are-we-singing-about'

def make_df(hot_100_weekly):

    #Initialize objects for grabbing overall word/tag frequencies and emotions
    tag_fd_total = FreqDist()
    affect_fq_total = {'positive':0, 'negative':0, 'anger':0, 'anticipation':0, 'disgust':0,
                   'fear':0, 'joy':0, 'sadness':0, 'surprise':0, 'trust':0}
    counter_affect_fq = Counter(affect_fq_total)   
    counter_word_fd_total = Counter(FreqDist()) 

    #Initialize final data product and loop through entries
    hot_100_product = {'date': hot_100_weekly['date']}
    data = []
    n_songs_lyrics_analyzed = 0
    for entry in hot_100_weekly['data']:

        #If available, grab duration
        duration = np.nan
        if 'duration' in entry:
            if (entry['duration'] != 0):
                duration = entry['duration']
        
        #If available, count Last.fm top tags
        if 'toptags' in entry:
            taglist = []
            for tag in entry['toptags']['tag']:
                taglist.append(tag['name'])
            tag_fd_total.update(taglist)

        #Keep a subset of the song data for the final product
        entry_new = {'song': entry['song'], 'artist': entry['artist'], 'this_week': entry['this_week'],
                     'peak_position': entry['peak_position'], 'weeks_on_chart': entry['weeks_on_chart'], 'duration': duration}
        
        n_words = np.nan
        if 'lyrics' in entry:

            #Preprocess lyrics:
            #1. Remove text in square brackets (designates sections)
            #2. Remove extra newlines to get rid of line groupings
            #4. Remove non-letter characters
            #5. Make lowercase
            lyrics = entry['lyrics']
            lyrics = re.sub(r'[\[].*?[\]]\n', '', lyrics)
            lyrics = re.sub(r'\n\n', '\n', lyrics)
            lyrics = re.sub(r'[^ \nA-Za-z0-9$/]+', '', lyrics)
            lyrics = lyrics.lower()

            #Only extract info for English songs
            try:
                lang = detect(lyrics)
            except:
                lang = None

            if (lang == 'en'):
                n_songs_lyrics_analyzed += 1

                #Then do the following:
                #1. Get number of lines
                #3. Instead of tokenizing, since we've removed non-letter characters, just split on spaces
                #2. Get number of words
                #4. Remove stop words and lemmatize 
                #5. Grab frequency and affect scores
                word_tokens = lyrics.replace('\n', '').split(' ')
                n_words = len(word_tokens)
                filtered_lemmatized_lyrics = []
                for w in word_tokens:
                    if w not in STOP_WORDS:
                        filtered_lemmatized_lyrics.append(WordNetLemmatizer().lemmatize(w))
                
                #Get FreqDist for song and divide by # of words before adding to total
                counter_word_fd = Counter(FreqDist(filtered_lemmatized_lyrics))
                for item, count in counter_word_fd.items():
                    counter_word_fd[item] /= n_words
                counter_word_fd_total = counter_word_fd_total + counter_word_fd

                text_object = NRCLex(lyrics)
                counter_affect_fq = counter_affect_fq  + Counter(text_object.affect_frequencies)
            
        entry_new['n_words'] = n_words
        data.append(entry_new)

    word_fd_avg = {}
    top_100_words = dict(counter_word_fd_total.most_common(100))
    for key in top_100_words:
        word_fd_avg[key] = top_100_words[key]/n_songs_lyrics_analyzed

    affect_fq_avg = {}
    for key in counter_affect_fq:
        affect_fq_avg[key] = counter_affect_fq[key]/n_songs_lyrics_analyzed

    hot_100_product['n_songs_lyrics_analyzed'] = n_songs_lyrics_analyzed
    hot_100_product['tag_fd'] = dict(tag_fd_total.most_common(50))
    hot_100_product['word_fd_avg'] = word_fd_avg
    hot_100_product['affect_fq_avg'] = affect_fq_avg

    hot_100_product['song'] = [entry['song'] for entry in data]
    hot_100_product['artist'] = [entry['artist'] for entry in data]
    hot_100_product['this_week'] = [entry['this_week'] for entry in data]
    hot_100_product['peak_position'] = [entry['peak_position'] for entry in data]
    hot_100_product['weeks_on_chart'] = [entry['weeks_on_chart'] for entry in data]
    hot_100_product['duration'] = [entry['duration'] for entry in data]
    hot_100_product['n_words'] = [entry['n_words'] for entry in data]

    return hot_100_product

def calc_confidence_wings(data, frac):
    sorted = np.sort(data[~np.isnan(data)])
    n_meas = len(sorted)
    loc_low = np.argmin(np.abs(np.arange(n_meas) - (1 - frac)*n_meas))
    loc_high = np.argmin(np.abs(np.arange(n_meas) - frac*n_meas))

    return sorted[loc_low], sorted[loc_high]

def extract_features(hot_100_product):

    df = pd.DataFrame.from_dict(hot_100_product).T
    n_weeks = len(df)

    #Get median/std song durations, converted to minutes
    confidence_frac = 0.84
    df['median_duration'] = 0
    df['err_duration_low'] = 0
    df['err_duration_high'] = 0
    df['median_wpm'] = 0
    df['err_wpm_low'] = 0
    df['err_wpm_high'] = 0
    df['median_weeks'] = 0
    df['err_weeks_low'] = 0
    df['err_weeks_high'] = 0

    df_artist_pop = df['date'].copy().to_frame()
    df_word_freq = df['date'].copy().to_frame()
    df_emotion = df['date'].copy().to_frame()
    for i in range(n_weeks):
        songs = df.iloc[i]['song']
        artists = df.iloc[i]['artist']
        this_week = df.iloc[i]['this_week']
        peak_position = df.iloc[i]['peak_position']
        artist_list = []

        #Grab artist in seperate list for counting popularity. If multiple artists, count both
        #Popularity is counted as 101 - chart position for each charting song
        for j in range(len(songs)):
            if ("&" in artists[j]):
                artist_split = re.split("&", artists[j])
                for name in artist_split:
                    if name not in df_artist_pop.columns:
                        df_artist_pop[name.strip()] = 0
                    df_artist_pop.iloc[i, df_artist_pop.columns.get_loc(name.strip())] += (101 - this_week[j])
            elif ("featuring" in artists[j].lower()):
                artist_split = re.split("featuring", artists[j], flags=re.IGNORECASE)
                for name in artist_split:
                    if name not in df_artist_pop.columns:
                        df_artist_pop[name.strip()] = 0
                    df_artist_pop.iloc[i, df_artist_pop.columns.get_loc(name.strip())] += (101 - this_week[j])
            else:
                if artists[j] not in df_artist_pop.columns:
                    df_artist_pop[artists[j]] = 0
                df_artist_pop.iloc[i, df_artist_pop.columns.get_loc(artists[j])] += (101 - this_week[j])
            
        #Grab other weekly metrics
        word_fd = df.iloc[i]['word_fd_avg']
        for key in word_fd:
            if (key != ''):
                if key not in df_word_freq.columns:
                    df_word_freq[key] = 0
                df_word_freq.iloc[i, df_word_freq.columns.get_loc(key)] = word_fd[key]
        emotion_fq = df.iloc[i]['affect_fq_avg']
        for key in emotion_fq:
            if key not in df_emotion.columns:
                df_emotion[key] = 0
            df_emotion.iloc[i, df_emotion.columns.get_loc(key)] = emotion_fq[key]

        duration = np.array(df.iloc[i]['duration']).astype('float')
        duration[np.where(duration == 0.)] = np.nan
        df.iloc[i, df.columns.get_loc('median_duration')] = np.nanmedian(duration)
        err_duration_low, err_duration_high = calc_confidence_wings(duration, confidence_frac)
        df.iloc[i, df.columns.get_loc('err_duration_low')] = err_duration_low
        df.iloc[i, df.columns.get_loc('err_duration_high')] = err_duration_high

        n_words = np.array(df.iloc[i]['n_words']).astype('float')
        n_words[np.where(n_words == 0.)] = np.nan
        wpm = n_words/duration
        df.iloc[i, df.columns.get_loc('median_wpm')] = np.nanmedian(wpm)
        err_wpm_low, err_wpm_high = calc_confidence_wings(wpm, confidence_frac)
        df.iloc[i, df.columns.get_loc('err_wpm_low')] = err_wpm_low
        df.iloc[i, df.columns.get_loc('err_wpm_high')] = err_wpm_high

        weeks = np.array(df.iloc[i]['weeks_on_chart']).astype('float')
        weeks[np.where(weeks == 0.)] = np.nan
        df.iloc[i, df.columns.get_loc('median_weeks')] = np.nanmedian(weeks)
        err_weeks_low, err_weeks_high = calc_confidence_wings(weeks, confidence_frac)
        df.iloc[i, df.columns.get_loc('err_weeks_low')] = err_weeks_low
        df.iloc[i, df.columns.get_loc('err_weeks_high')] = err_weeks_high

    df['median_duration'] = df['median_duration']/60000
    df['err_duration_low'] = df['err_duration_low']/60000
    df['err_duration_high'] = df['err_duration_high']/60000
    df['median_wpm'] = df['median_wpm']*60000
    df['err_wpm_low'] = df['err_wpm_low']*60000
    df['err_wpm_high'] = df['err_wpm_high']*60000

    return df, df_artist_pop, df_word_freq, df_emotion

def lambda_handler(event, context):
    s3 = boto3.resource('s3')
    obj = s3.Object(BUCKET, f"data/hot-100-{datetime.today().date()}.json")
    hot_100 = json.loads(obj.get()['Body'].read().decode('UTF-8'))

    #Extract/transform functions are set up to make it easy to extract multiple weeks' worth of data at a time
    #But in this script we are just extracting one week at a time
    hot_100_product = make_df(hot_100)
    hot_100_all = {}
    dt_formatted = str(datetime.strptime(hot_100['date'], '%Y-%m-%d'))
    hot_100_product['date'] = dt_formatted
    hot_100_all[dt_formatted] = hot_100_product
    df_final_today, df_artist_pop_today, df_word_freq_today, df_emotion_today = extract_features(hot_100_all)

    #Append this week to tables
    df = pd.read_json(s3.Object('what-are-we-singing-about', 'df_final.json').get()['Body'].read().decode('UTF-8'))
    df_artist_pop = pd.read_json(s3.Object('what-are-we-singing-about', 'df_artist_pop.json').get()['Body'].read().decode('UTF-8'))
    df_word_freq = pd.read_json(s3.Object('what-are-we-singing-about', 'df_word_freq.json').get()['Body'].read().decode('UTF-8'))
    df_emotion = pd.read_json(s3.Object('what-are-we-singing-about', 'df_emotion.json').get()['Body'].read().decode('UTF-8'))

    key = 'df_final.json'
    obj = s3.Object(BUCKET, key)
    df = pd.read_json(obj.get()['Body'].read().decode('UTF-8'))
    df = df.append(df_final_today)
    json_buffer = io.StringIO()
    df.to_json(json_buffer)
    #s3.Bucket(BUCKET).put_object(Key=key, Body=json_buffer.getvalue())

    key = 'df_artist_pop.json'
    obj = s3.Object(BUCKET, key)
    df = pd.read_json(obj.get()['Body'].read().decode('UTF-8'))
    df = df.append(df_artist_pop_today).fillna(0)
    json_buffer = io.StringIO()
    df.to_json(json_buffer)
    #s3.Bucket(BUCKET).put_object(Key=key, Body=json_buffer.getvalue())

    key = 'df_word_freq.json'
    obj = s3.Object(BUCKET, key)
    df = pd.read_json(obj.get()['Body'].read().decode('UTF-8'))
    df = df.append(df_word_freq_today).fillna(0)
    json_buffer = io.StringIO()
    df.to_json(json_buffer)
    #s3.Bucket(BUCKET).put_object(Key=key, Body=json_buffer.getvalue())

    key = 'df_emotion.json'
    obj = s3.Object(BUCKET, key)
    df = pd.read_json(obj.get()['Body'].read().decode('UTF-8'))
    df = df.append(df_emotion_today).fillna(0)
    json_buffer = io.StringIO()
    df.to_json(json_buffer)
    #s3.Bucket(BUCKET).put_object(Key=key, Body=json_buffer.getvalue())

if __name__ == '__main__':
    lambda_handler(None,None)