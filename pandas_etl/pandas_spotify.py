import pandas as pd
import requests
import datetime
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from dotenv import load_dotenv
import base64
import os

load_dotenv()

class Pandas_ETL():
    CLIENT_ID = os.environ.get('SP_CLIENT_ID')
    CLIENT_SEC = os.environ.get('SP_CLIENT_SECRET')

    def get_data(self):
        scope = 'user-library-read user-read-recently-played'

        today = datetime.datetime.now()
        yesterday = today - datetime.timedelta(days = 1)
        yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

        sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=self.CLIENT_ID,
        client_secret=self.CLIENT_SEC,
        redirect_uri='http://localhost:3000/callback',
        scope=scope))

        return sp.current_user_recently_played(after=yesterday_unix_timestamp, limit=40)

    def extract(self):
        data = self.get_data()

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
            'song_name': song_names,
            'artist_name': artist_names,
            'played_at': played_at,
            'timestamps': timestamps
        }

        song_df = pd.DataFrame(song_dict, columns=['song_name', 'artist_name', 'played_at', 'timestamps'])

        return song_df

    def transform_load(self):
        df = self.extract()

        # Check if our dataframe is empty
        if df.empty:
            print('No Songs Downloaded. Finishing Execution')
            return False
        
        # Primary Key Check
        if pd.Series(df['played_at']).is_unique:
            pass
        else:
            raise Exception(f'[Transform Error]: Primary Key Check not valid - Duplicated data in {df.columns.tolist()[2]}')

        if df.isnull().values.any():
            raise Exception('No Real Values Found')

        # Check that all timestamps are of yesterday's date - past 24 hours
        yesterday = datetime.datetime.now() - datetime.timedelta(days = 1)
        yesterday = yesterday.replace(hour=0,minute=0,second=0,microsecond=0)

        timestamps = df['timestamps'].tolist()
        for timestamp in timestamps:
            if datetime.datetime.strptime(timestamp, '%Y-%m-%d') != yesterday:
                raise Exception(f'[Transform Error]: At least one or more of the returned songs does not come within the last 24 hours')

        # LOAD
        DATABASE_URI = os.environ.get('DATABASE_URL')

        try:
            df.to_sql('my_played_track_history', con=DATABASE_URI, index=False, if_exists='append')
        except:
            print('Data Already exists in Database')
        
        print('Closing Connection...')