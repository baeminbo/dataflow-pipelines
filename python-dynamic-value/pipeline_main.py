import logging

from apache_beam import Create
from apache_beam import DoFn
from apache_beam import ParDo
from apache_beam import Pipeline
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.value_provider import NestedValueProvider
from apache_beam.options.value_provider import StaticValueProvider
from apache_beam.options.value_provider import ValueProvider


class MyOptions(PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument('--today',
                        type=str,
                        help='A date value')


class MyDoFn(DoFn):
  def __init__(self, tag):
    # type: (ValueProvider) -> None
    """
    :param tag (ValueProvider): A string to print in logs.
    """
    self._tag = tag

  def process(self, element):
    logging.info(f'[{self._tag.get()}] element: {element}')


def main():
  options = PipelineOptions()
  options.view_as(SetupOptions).save_main_session = True

  today = options.view_as(MyOptions).today  # type: ValueProvider

  # Create a ValueProvider (NestedValueProvider) from another ValueProvider
  # (StaticValueProvider)
  tag = NestedValueProvider(
      StaticValueProvider(str, today),
      lambda v: f'TODAY:{v}')  # type: ValueProvider

  pipeline = Pipeline(options=options)
  (pipeline
   | Create(range(10), reshuffle=False)
   | 'Print' >> ParDo(MyDoFn(tag)))

  pipeline.run()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  main()
