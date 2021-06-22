import logging

import apache_beam as beam
from apache_beam import Pipeline
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

CONNECTION_STRING = (
    'DRIVER=MySQL ODBC 8.0 ANSI Driver;'
    'SERVER=********;'
    'DATABASE=****;'
    'UID=****;'
    'PWD=****;'
    'charset=utf8mb4;'
)


def connect_and_query(ignored_input):
  import pyodbc
  # From https://github.com/mkleehammer/pyodbc/wiki/Connecting-to-MySQL
  cnxn = pyodbc.connect(CONNECTION_STRING)
  result = cnxn.execute('SELECT * FROM table1')
  logging.info('result: %s', result)

def run(argv=None):
  options = PipelineOptions(argv)
  options.view_as(SetupOptions).save_main_session = True

  with Pipeline(options=options) as p:
    (p
     | beam.Create([None])
     | beam.ParDo(connect_and_query))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
