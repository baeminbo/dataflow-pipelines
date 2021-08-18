import logging
import os

from apache_beam import Create
from apache_beam import ParDo
from apache_beam import Pipeline
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


_ELEMENTS_PER_INPUT = 1000000

class _Options(PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument(
        '--inputs', type=int, default=1000,
        help='Number of inputs. Default is 1000.')
    parser.add_argument(
        '--output_prefix', type=str, default='',
        help='Output location. For example, "gs://<BUCKET>/output-". Default '
             'is "<temp_location>/output-"')
    parser.add_argument(
        '--shards', type=int, default=0,
        help='Number of shards writing to files. By default, decided by the '
             'service')


def main():
  options = PipelineOptions()
  options.view_as(SetupOptions).save_main_session = True

  opt = options.view_as(_Options)
  inputs = opt.inputs
  output_prefix = opt.output_prefix or os.path.join(
      options.view_as(GoogleCloudOptions).temp_location, 'output')
  shards = opt.shards

  p = Pipeline(options=options)

  def generate(n):
    yield from range(n * _ELEMENTS_PER_INPUT, (n + 1) * _ELEMENTS_PER_INPUT )

  (p
   | Create(range(inputs))
   | ParDo(generate).with_output_types(int)
   | WriteToText(output_prefix, num_shards=shards))

  p.run()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  main()
