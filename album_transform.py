import logging
import apache_beam as beam
import re

from datetime import date
from apache_beam.io import ReadFromParquet, ReadFromBigQuery, WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
from typing import NamedTuple

class Album(NamedTuple):
    album_id: str
    name: str
    release_date: date
    album_type: str

    def to_dict(self):
        return {
            'album_id': self.album_id,
            'name': self.name,
            'release_date': self.reformat_release_date(self.release_date),
            'album_type': self.album_type
        }

    def reformat_release_date(self, release_date):
        if re.match(r"^\d*-\d*$", release_date):
            release_date = f"{release_date}-01"

        return release_date

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
        from core.dim_albums
    """
    table_schema = 'album_id:STRING,name:STRING,release_date:DATE,album_type:STRING'
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
            | 'To NamedTuple' >> beam.Map(lambda item: Album(**item)).with_output_types(Album)
            | 'Deduplicate elements' >> beam.Distinct()
            | 'Map New Data' >> beam.Map(lambda item: (item.album_id, item))
        )

        existing_data_map = (
            existing_data
            | 'To NamedTuple(Existing)' >> beam.Map(lambda item: Album(**item)).with_output_types(Album)
            | 'Map Existing Data' >> beam.Map(lambda item: (item.album_id, item))
        )

        joined = (({
            'existing': existing_data_map,
            'new_data': new_data_map
        })
        | 'Merge' >> beam.CoGroupByKey()
        | 'Filter Old Records' >> beam.Filter(lambda data: len(data[1]['existing']) == 0)
        | 'Map For New Records' >> beam.Map(lambda data: data[1]['new_data'][0])
        | 'Map to Tuple' >> beam.Map(lambda album: album_to_dict(album))
        )

        joined | 'Write' >> WriteToBigQuery(
            table='dim_albums',
            dataset='core',
            temp_file_format='AVRO',
            schema=table_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
        )

def album_to_dict(album):
    return {
        'album_id': album.album_id,
        'name': album.name,
        'release_date': reformat_release_date(album.release_date),
        'album_type': album.album_type
    }

def reformat_release_date(release_date):
    if re.match(r"^\d*-\d*$", release_date): # Only year-month format. Ex: 2020-01
        release_date = f"{release_date}-01"
    elif re.match(r"^\d{4}$", release_date): # Only year format. Example: 2000
        release_date = f"{release_date}-01-01"

    return release_date


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
