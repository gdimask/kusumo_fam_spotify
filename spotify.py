import spotipy
import os
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
from spotipy.oauth2 import SpotifyOAuth
from spotipy.oauth2 import SpotifyClientCredentials
from googleapiclient.discovery import build
import hashlib
# from flask import Request
from google.cloud import firestore
import pdb
import logging

load_dotenv()

def get_spotify_data() -> list:
    ja = pd.read_parquet('temps_track_df_2022-09-21.parquet')
    breakpoint()

if __name__ == '__main__':
    get_spotify_data()
