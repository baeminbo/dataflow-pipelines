import logging

import apache_beam as beam

from apache_beam import Pipeline
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from mysql.connector import connection


class MysqlDoFn(beam.DoFn):
  def __init__(self):
    self.conn = None

  def start_bundle(self):
    # From https://dev.mysql.com/doc/connector-python/en/connector-python-example-connecting.html
    self.conn = connection.MySQLConnection(user='****', passwd='********',
                                           host='********', database='****')

  def process(self, element):
    cursor = self.conn.cursor()
    try:
      cursor.execute('SELECT * FROM table1')
      for row in cursor:
        logging.info('row: %s', row)
    finally:
      cursor.close()

  def finish_bundle(self):
    if self.conn is not None:
      self.conn.close()
      self.conn = None


def run(argv=None):
  options = PipelineOptions(argv)
  options.view_as(SetupOptions).save_main_session = True
  with Pipeline(options=options) as p:
    (p
     | beam.Impulse()
     | beam.ParDo(MysqlDoFn()))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
