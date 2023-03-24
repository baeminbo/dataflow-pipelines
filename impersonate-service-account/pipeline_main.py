import logging

from apache_beam import DoFn
from apache_beam import ParDo
from apache_beam import Pipeline
from apache_beam.io import ReadFromBigQuery
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions


class PrintDoFn(DoFn):
  def process(self, element):
    logging.info(f'element: {element}')


def main():
  options = PipelineOptions()
  pipeline = Pipeline(options=options)

  project = options.view_as(GoogleCloudOptions).project
  table = f'{project}.dataset1.table1'

  (pipeline
   | ReadFromBigQuery(
          query=f'SELECT * FROM `{table}`',
          use_standard_sql=True)
   | ParDo(PrintDoFn()))

  pipeline.run()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  main()
