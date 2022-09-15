CREATE TABLE core.dim_albums(
  album_id STRING,
  name STRING,
  release_date DATE,
  album_type STRING,
);

CREATE TABLE core.dim_artists(
  artist_id STRING,
  name STRING,
  artist_type STRING,
);

CREATE TABLE core.dim_tracks(
  track_id STRING,
  name STRING,
  popularity INTEGER,
  track_type STRING,
);

CREATE TABLE core.fct_plays(
  play_id STRING,
  track_id STRING,
  album_id STRING,
  artist_id STRING,
  played_at DATETIME,
);

