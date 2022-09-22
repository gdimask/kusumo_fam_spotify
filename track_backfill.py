import logging
import os
import spotipy
import pandas_gbq
import pandas as pd
from dotenv import load_dotenv
from spotipy.oauth2 import SpotifyClientCredentials

HEADERS = {
    'track_id': 'track_id',
    'name': 'name',
    'popularity': 'popularity',
    'track_type': 'track_type',
    'danceability_right': 'danceability',
    'energy_right': 'energy',
    'speechiness_right': 'speechiness',
    'acousticness_right': 'acousticness',
    'instrumentalness_right': 'instrumentalness',
    'liveness_right': 'liveness',
    'valence_right': 'valence',
    'tempo_right': 'tempo',
    'key_right': 'key',
    'duration_ms_right': 'duration_ms',
}

load_dotenv()
def backfill(request):
    project_id = os.environ.get('PROJECT_ID')
    query = """
        select *
        from core.dim_tracks
    """
    tracks = pd.read_gbq(query, project_id=project_id)
    track_ids = tracks['track_id']
    headers = ['id', 'danceability', 'energy', 'speechiness', 'loudness', 'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo', 'key', 'duration_ms']

    features = []
    spotify_client_id = os.environ.get('SPOTIPY_CLIENT_ID')
    spotify_client_secret = os.environ.get('SPOTIPY_CLIENT_SECRET')
    client_credentials_manager = SpotifyClientCredentials(
        client_id=spotify_client_id,
        client_secret=spotify_client_secret)

    spotify_client = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    for track_id in track_ids:
        response = spotify_client.audio_features([track_id])[0]
        feature_list = []
        for header in headers:
            feature_list.append(response.get(header))

        audio_feature = tuple(feature_list)
        features.append(audio_feature)

    audio_feature_df = pd.DataFrame(features, columns=headers)
    new_tracks = tracks.merge(audio_feature_df, how='left', left_on='track_id', right_on='id', suffixes=('_left', '_right'))
    new_tracks = new_tracks[HEADERS.keys()]
    final_tracks_df = new_tracks.rename(columns=HEADERS)
    pandas_gbq.to_gbq(
        final_tracks_df, 'development.dim_tracks', project_id=project_id, if_exists='replace',
    )

    print('Success')



if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    backfill({})
