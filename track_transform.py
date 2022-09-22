import logging
import apache_beam as beam

from apache_beam.io import ReadFromParquet, ReadFromBigQuery, WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
from typing import NamedTuple


class Track(NamedTuple):
    track_id: str
    name: str
    popularity: int
    track_type: str
    danceability: float
    energy: float
    speechiness: float
    acousticness: float
    instrumentalness: float
    liveness: float
    valence: float
    tempo: float
    key: int
    duration_ms: int

class RunTimeOptions(PipelineOptions):
   @classmethod
   def _add_argparse_args(cls, parser):
       parser.add_value_provider_argument(
           "--input",
           help="uri of the input file"
       )

def run():
    pipeline_options = PipelineOptions(streaming=False, save_main_session=True, region='us')
    user_options = pipeline_options.view_as(RunTimeOptions)

    query = """
        select *
        from core.dim_tracks
    """
    table_schemas = [
        'track_id:STRING',
        'name:STRING',
        'popularity:INTEGER',
        'track_type:STRING',
        'danceability:FLOAT',
        'energy:FLOAT',
        'speechiness:FLOAT',
        'acousticness:FLOAT',
        'instrumentalness:FLOAT',
        'liveness:FLOAT',
        'valence:FLOAT',
        'tempo:FLOAT',
        'key:INTEGER',
        'duration_ms:INTEGER'
    ]
    table_schema = ','.join(table_schemas)
    with beam.Pipeline(options=pipeline_options) as p:
        new_data = p | 'ReadAlbum' >> ReadFromParquet(user_options.input)

        existing_data = (
            p
            | 'Read Existing Data' >> ReadFromBigQuery(
                method='DIRECT_READ',
                query=query,
                use_standard_sql=True,
            )
        )

        new_data_map = (
            new_data
            | 'To NamedTuple' >> beam.Map(lambda item: Track(**item)).with_output_types(Track)
            | 'Deduplicate elements' >> beam.Distinct()
            | 'Map New Data' >> beam.Map(lambda item: (item.track_id, item))
        )

        existing_data_map = (
            existing_data
            | 'To NamedTuple(Existing)' >> beam.Map(lambda item: Track(**item)).with_output_types(Track)
            | 'Map Existing Data' >> beam.Map(lambda item: (item.track_id, item))
        )

        joined = (({
            'existing': existing_data_map,
            'new_data': new_data_map
        })
        | 'Merge' >> beam.CoGroupByKey()
        | 'Filter Old Records' >> beam.Filter(lambda data: len(data[1]['existing']) == 0)
        | 'Map For New Records' >> beam.Map(lambda data: data[1]['new_data'][0])
        | 'Map to Tuple' >> beam.Map(lambda track: track_to_dict(track))
        )

        joined | 'Write' >> WriteToBigQuery(
            table='dim_tracks',
            dataset='core',
            temp_file_format='AVRO',
            schema=table_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
        )

def track_to_dict(track):
    return {
        'track_id': track.track_id,
        'name': track.name,
        'popularity': track.popularity,
        'track_type': track.track_type,
        'danceability': track.danceability,
        'energy': track.energy,
        'speechiness': track.speechiness,
        'acousticness': track.acousticness,
        'instrumentalness': track.instrumentalness,
        'liveness': track.liveness,
        'valence': track.valence,
        'tempo': track.tempo,
        'key': track.key,
        'duration_ms': track.duration_ms
    }

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
