import logging
from typing import Iterable
from typing import Type

from apache_beam import Create
from apache_beam import ParDo
from apache_beam import Pipeline
from apache_beam import pvalue
from apache_beam.options.pipeline_options import PipelineOptions

_NUM_ELEMENTS = 100000


class SideInputOptions(PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument('--side_type',
                        type=str,
                        default='iter',
                        help='iter (default) or list.')
    parser.add_argument('--num_elements',
                        type=int,
                        default=_NUM_ELEMENTS,
                        help=f'number of elements. Default is {_NUM_ELEMENTS}.')


def get_sideinput_fn(options: PipelineOptions) -> Type[pvalue.AsSideInput]:
  side_type = options.view_as(SideInputOptions).side_type

  if side_type == 'iter':
    return pvalue.AsIter
  elif side_type == 'list':
    return pvalue.AsList
  else:
    raise ValueError(f'Unexpected side_type: {side_type}')


def get_num_elements(options: PipelineOptions):
  return options.view_as(SideInputOptions).num_elements


def print_kv(element: int, side: Iterable[str]):
  logging.info(
      f"print_kv element: {element}, side ({type(side)}): {list(side)}.")


def main():
  options = PipelineOptions()
  side_type: pvalue.AsSideInput

  pipeline = Pipeline(options=options)

  side = pipeline | "Side" >> Create(["a", "b", "c"])

  (pipeline
   | "Main" >> Create(range(get_num_elements(options)))
   | "Print" >> ParDo(print_kv, side=get_sideinput_fn(options)(side)))

  pipeline.run()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  main()
