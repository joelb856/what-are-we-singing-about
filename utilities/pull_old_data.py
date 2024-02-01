import numpy as np
import json
import requests
import boto3
import io
from datetime import date, timedelta

from lambda_function import pull_data
from extract_features import make_df, calc_confidence_wings, extract_features

BUCKET = 'what-are-we-singing-about'
HOT_100_HISTORIC_BASE = 'https://raw.githubusercontent.com/mhollingshead/billboard-hot-100/main/date/'
VALID_DATES_URL = 'https://raw.githubusercontent.com/mhollingshead/billboard-hot-100/main/valid_dates.json'

date_start = '2020-01-01'

def pull_old_data(date_start):
    valid_dates = requests.get(VALID_DATES_URL).json()

    for date_valid in valid_dates:
        if date.fromisoformat(date_valid) > date.fromisoformat(date_start):
            pull_data(date_string=date_valid)

if __name__ == "__main__":
    #pull_old_data(date_start)

    s3 = boto3.resource('s3')
    client = boto3.client('s3')
    all_objects = client.list_objects_v2(Bucket=BUCKET, Prefix='data/')
    hot_100_all = {}
    for object_info in all_objects['Contents']:
        if ".json" in object_info['Key']:
            print(object_info['Key'])
            obj = s3.Object(BUCKET, object_info['Key'])
            hot_100 = json.loads(obj.get()['Body'].read().decode('UTF-8'))
            hot_100_product = make_df(hot_100)
            hot_100_all[hot_100_product['date']] = hot_100_product

    df_final, df_artist_pop, df_word_freq, df_emotion = extract_features(hot_100_all)

    json_buffer = io.StringIO()
    df_final.to_json(json_buffer)
    s3.Bucket(BUCKET).put_object(Key='df_final.json', Body=json_buffer.getvalue())

    json_buffer = io.StringIO()
    df_artist_pop.to_json(json_buffer)
    s3.Bucket(BUCKET).put_object(Key='df_artist_pop.json', Body=json_buffer.getvalue())

    json_buffer = io.StringIO()
    df_word_freq.to_json(json_buffer)
    s3.Bucket(BUCKET).put_object(Key='df_word_freq.json', Body=json_buffer.getvalue())

    json_buffer = io.StringIO()
    df_emotion.to_json(json_buffer)
    s3.Bucket(BUCKET).put_object(Key='df_emotion.json', Body=json_buffer.getvalue())