import logging
import apache_beam as beam

from apache_beam.io import ReadFromParquet, ReadFromBigQuery, WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
from typing import NamedTuple

class Artist(NamedTuple):
    artist_id: str
    name: str
    artist_type: str

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
        from core.dim_artists
    """
    table_schema = 'artist_id:STRING,name:STRING,artist_type:STRING'
    with beam.Pipeline(options=pipeline_options) as p:
        new_data = p | 'ReadArtist' >> ReadFromParquet(user_options.input)

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
            | 'To NamedTuple' >> beam.Map(lambda item: Artist(**item)).with_output_types(Artist)
            | 'Deduplicate elements' >> beam.Distinct()
            | 'Map New Data' >> beam.Map(lambda item: (item.artist_id, item))
        )

        existing_data_map = (
            existing_data
            | 'To NamedTuple(Existing)' >> beam.Map(lambda item: Artist(**item)).with_output_types(Artist)
            | 'Map Existing Data' >> beam.Map(lambda item: (item.artist_id, item))
        )

        joined = (({
            'existing': existing_data_map,
            'new_data': new_data_map
        })
        | 'Merge' >> beam.CoGroupByKey()
        | 'Filter Old Records' >> beam.Filter(lambda data: len(data[1]['existing']) == 0)
        | 'Map For New Records' >> beam.Map(lambda data: data[1]['new_data'][0])
        | 'Map to Tuple' >> beam.Map(lambda artist: artist_to_dict(artist))
        )

        joined | 'Write' >> WriteToBigQuery(
            table='dim_artists',
            dataset='core',
            temp_file_format='AVRO',
            schema=table_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
        )

def artist_to_dict(artist):
    return {
        'artist_id': artist.artist_id,
        'name': artist.name,
        'artist_type': artist.artist_type
    }

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
