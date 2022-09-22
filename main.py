import spotipy
import os
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
from spotipy.oauth2 import SpotifyOAuth, SpotifyClientCredentials
from googleapiclient.discovery import build
import hashlib
# from flask import Request
from google.cloud import firestore
import pdb
import logging

load_dotenv()

def extract_spotify(request):
    logging.getLogger().setLevel(logging.INFO)
    recently_played = get_spotify_data()

    album_df = create_albums_dataframe(recently_played)
    artist_df = create_artists_dataframe(recently_played)
    track_df = create_tracks_dataframe(recently_played)
    play_df = create_plays_dataframe(recently_played)

    load_dataframe('album', album_df)
    load_dataframe('artist', artist_df)
    load_dataframe('track', track_df)
    load_dataframe('play', play_df)
    print('Success')

def get_spotify_data() -> list:
    spotify_client_id = os.environ.get('SPOTIPY_CLIENT_ID')
    spotify_client_secret = os.environ.get('SPOTIPY_CLIENT_SECRET')
    spotify_redirect_url = os.environ.get('SPOTIPY_REDIRECT_URI')

    spotipy_auth = SpotifyOAuth(
        client_id=spotify_client_id,
        client_secret=spotify_client_secret,
        redirect_uri=spotify_redirect_url,
        scope="user-read-recently-played")

    spotipy_obj = spotipy.Spotify(auth_manager=spotipy_auth)
    recently_played = spotipy_obj.current_user_recently_played(limit=50)
    return recently_played

def create_albums_dataframe(recently_played: dict) -> pd.DataFrame:
    albums = []
    album_columns = ['album_id', 'name', 'release_date', 'album_type']

    for item in recently_played.get('items'):
        album = item.get('track').get('album')
        album_data = [album.get('id'), album.get('name'), album.get(
            'release_date'), album.get('album_type')]

        albums.append(album_data)

    album_df = pd.DataFrame(albums, columns=album_columns)

    return album_df


def create_artists_dataframe(recently_played: dict) -> pd.DataFrame:
    artists = []
    artist_columns = ['artist_id', 'name', 'artist_type']

    for item in recently_played.get('items'):
        artist = item.get('track').get('album').get('artists')[0]
        artist_data = [artist.get('id'), artist.get(
            'name'), artist.get('type')]
        artists.append(artist_data)

    artist_df = pd.DataFrame(artists, columns=artist_columns)

    return artist_df


def create_tracks_dataframe(recently_played: dict) -> pd.DataFrame:
    tracks = []
    track_columns = ['track_id', 'name', 'popularity', 'track_type', 'danceability', 'energy', 'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo', 'key', 'duration_ms']

    for item in recently_played.get('items'):
        track = item.get('track')
        audio_feature = get_audio_feature(track)
        audio_feature_headers = ['danceability', 'energy', 'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo', 'key', 'duration_ms']
        track_data = [track.get('id'), track.get(
            'name'), track.get('popularity'), track.get('type'),
            ]
        for audio_feature_header in audio_feature_headers:
            track_data.append(audio_feature.get(audio_feature_header))

        tracks.append(track_data)

    track_df = pd.DataFrame(tracks, columns=track_columns)

    return track_df

def get_audio_feature(track: dict):
    spotify_client_id = os.environ.get('SPOTIPY_CLIENT_ID')
    spotify_client_secret = os.environ.get('SPOTIPY_CLIENT_SECRET')
    client_credentials_manager = SpotifyClientCredentials(
        client_id=spotify_client_id,
        client_secret=spotify_client_secret)

    spotify_client = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    response = spotify_client.audio_features([track.get('id')])[0]

    return response


def create_plays_dataframe(recently_played: dict) -> pd.DataFrame:
    plays = []
    play_columns = ['play_id', 'track_id', 'album_id', 'artist_id', 'played_at']

    for item in recently_played.get('items'):
        track = item.get('track')
        artist = item.get('track').get('album').get('artists')[0]
        album = item.get('track').get('album')
        track_key = f"{track.get('id')}-{item.get('played_at')}"
        play_id = hashlib.md5(track_key.encode()).hexdigest()

        play_data = [play_id, track.get('id'), album.get('id'), artist.get(
            'id'), item.get('played_at')]
        plays.append(play_data)

    play_df = pd.DataFrame(plays, columns=play_columns)

    return play_df

def load_dataframe(table_type: str, df: pd.DataFrame) -> None:
    filename = create_filename(table_type)
    df.to_parquet(filename)
    launch_dataflow_job(table_type, filename)

def create_filename(table_type: str) -> str:
    current_datetime = datetime.now().strftime("%Y-%m-%d")
    return f"gs://{os.environ.get('BUCKET_NAME')}/temps/{table_type}_df_{current_datetime}.parquet"

def launch_dataflow_job(table_type: str, filename: str) -> None:
    dataflow = build('dataflow', 'v1b3')
    job_name = f"{table_type}-flow"
    request = dataflow.projects().locations().templates().launch(
        projectId=os.environ.get('PROJECT_ID'),
        gcsPath=f"gs://{os.environ.get('BUCKET_NAME')}/{table_type}_transfrom",
        location='us-west2',
        body={
            "environment": {
                "temp_location": f"gs://{os.environ.get('BUCKET_NAME')}/tmp"
            },
            "parameters": {
                "input": filename
            },
            "jobName": job_name
        }
    )

    request.execute()
    print(f"{job_name} Launched!")

if __name__ == '__main__':
    extract_spotify({})
