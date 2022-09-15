from datetime import datetime
import logging
import apache_beam as beam

from apache_beam.io import ReadFromParquet, ReadFromBigQuery, WriteToBigQuery, WriteToText, WriteToAvro
from apache_beam.options.pipeline_options import PipelineOptions
from typing import NamedTuple

class Play(NamedTuple):
    play_id: str
    track_id: str
    album_id: str
    artist_id: str
    played_at: datetime

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
        from core.fct_plays
    """
    table_schema = 'play_id:STRING,track_id:STRING,album_id:STRING,artist_id:STRING,played_at:DATETIME'
    with beam.Pipeline(options=pipeline_options) as p:
        # new_data = p | 'ReadCSV' >> ReadFromParquet('gs://grego-spotify/temps/play_df_2022-09-11.parquet')
        new_data = p | 'ReadCSV' >> ReadFromParquet(user_options.input)

        existing_data = (
            p
            | 'Read Existing Data' >> ReadFromBigQuery(
                method='DIRECT_READ',
                query=query,
                project='dbtbigquery-331216',
                use_standard_sql=True,
            )
        )

        new_data_map = (
            new_data
            | 'To NamedTuple' >> beam.Map(lambda item: Play(**item)).with_output_types(Play)
            | 'Deduplicate elements' >> beam.Distinct()
            | 'Map New Data' >> beam.Map(lambda item: (item.play_id, item))
        )

        existing_data_map = (
            existing_data
            | 'To NamedTuple(Existing)' >> beam.Map(lambda item: Play(**item)).with_output_types(Play)
            | 'Map Existing Data' >> beam.Map(lambda item: (item.play_id, item))
        )

        joined = (({
            'existing': existing_data_map,
            'new_data': new_data_map
        })
        | 'Merge' >> beam.CoGroupByKey()
        | 'Filter Old Records' >> beam.Filter(lambda data: len(data[1]['existing']) == 0)
        | 'Map For New Records' >> beam.Map(lambda data: data[1]['new_data'][0])
        | 'Map to Tuple' >> beam.Map(lambda play: play_to_dict(play))
        )
        # joined | WriteToAvro('test.avro', table_schema)

        joined | 'Write' >> WriteToBigQuery(
            table='fct_plays',
            dataset='core',
            schema=table_schema,
            project='dbtbigquery-331216',
            custom_gcs_temp_location='gs://grego-spotify/beam_temp',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
        )

def play_to_dict(play):

    return {
        'play_id': play.play_id,
        'track_id': play.track_id,
        'album_id': play.album_id,
        'artist_id': play.artist_id,
        'played_at': datetime.strptime(play.played_at, '%Y-%m-%dT%H:%M:%S.%fZ')
    }

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
