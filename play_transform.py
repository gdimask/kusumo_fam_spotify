from datetime import datetime
import logging
import apache_beam as beam

from apache_beam.io import ReadFromParquet, WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
from typing import NamedTuple

class Play(NamedTuple):
    track_id: str
    album_id: str
    artist_id: str
    played_at: datetime
    context: str

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

    table_schema = 'track_id:STRING,album_id:STRING,artist_id:STRING,played_at:TIMESTAMP'
    with beam.Pipeline(options=pipeline_options) as p:
        new_data = p | 'ReadAlbum' >> ReadFromParquet(user_options.input)

        new_data_map = (
            new_data
            | 'To NamedTuple' >> beam.Map(lambda item: Play(**item)).with_output_types(Play)
            | 'Map New Data' >> beam.Map(lambda item: play_to_dict(item))
        )

        new_data_map | 'Write' >> WriteToBigQuery(
            table='fct_plays',
            dataset='core',
            temp_file_format='AVRO',
            schema=table_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
        )

def play_to_dict(play):
    return {
        'track_id': play.track_id,
        'album_id': play.album_id,
        'artist_id': play.artist_id,
        'played_at': datetime.strptime(play.played_at, '%Y-%m-%dT%H:%M:%S.%fZ')
    }

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
