import spotipy
from spotipy.oauth2 import SpotifyOAuth
import datetime
import os

from pyspark.sql import SparkSession

import databricks.koalas as ks
from dotenv import load_dotenv

load_dotenv()

# Extract Data
def extract():
    CLIENT_ID = os.environ.get('SP_CLIENT_ID')
    CLIENT_SEC = os.environ.get('SP_CLIENT_SECRET')

    scope = "user-library-read user-read-recently-played"

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days = 1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=CLIENT_ID,client_secret=CLIENT_SEC,redirect_uri='http://localhost:3000/callback',scope=scope))

    return sp.current_user_recently_played(after=yesterday_unix_timestamp,limit=40)

def transform_load():
    data = extract()

    # Check if data is empty
    if data is None:
        print('No Songs Downloaded. Finishing Execution')
        return False
    
    song_names = []
    artist_names = []
    played_at = []
    timestamps = []

    for song in data['items']:
        song_names.append(song['track']['name'])
        artist_names.append(song['track']['album']['artists'][0]['name'])
        played_at.append(song['played_at'])
        timestamps.append(song['played_at'][0:10])

    song_dict = {
        'song_names': song_names,
        'artist_names': artist_names,
        'played_at': played_at,
        'timestamps': timestamps
    }

    spark = SparkSession.builder.appName('Spotify').getOrCreate()

    sc = spark.sparkContext

    ks_data = ks.DataFrame(song_dict)

    # Primary Key Check
    if ks_data['played_at'].is_unique:
        pass
    else:
        raise Exception(f'[TRANSFORM ERROR]: Primary Key Check not valid - Duplicated Values in {ks_data.columns.tolist()[2]}')


    # Create CSV File from the Spotify DataFrame Data
    ks_data.to_csv('./out/song_data', mode='append', num_files=1)

transform_load()