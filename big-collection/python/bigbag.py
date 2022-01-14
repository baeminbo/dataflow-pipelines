import logging
from typing import Tuple

from apache_beam import Create
from apache_beam import DoFn
from apache_beam import FlatMap
from apache_beam import ParDo
from apache_beam import Pipeline
from apache_beam import TimeDomain
from apache_beam import WithKeys
from apache_beam import typehints
from apache_beam.coders import BytesCoder
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.transforms.userstate import BagStateSpec
from apache_beam.transforms.userstate import TimerSpec
from apache_beam.transforms.userstate import on_timer

# The total bytes processed is NUM_SHARDS * NUM_ELEMENTS_PER_SHARD * ELEMENT_BYTES ~= 3 GiB
NUM_SHARDS = 100
NUM_ELEMENTS_PER_SHARD = 10
ELEMENT_BYTES = 3 * 1024 * 1024  # 3 MiB


@typehints.with_input_types(Tuple[str, bytes])
@typehints.with_output_types(None)
class BigBagDoFn(DoFn):
  VALUES_STATE = BagStateSpec('values', BytesCoder())
  END_OF_WINDOW_TIMER = TimerSpec('end_of_window', TimeDomain.WATERMARK)

  def process(self, element: Tuple[str, bytes], window=DoFn.WindowParam,
      values_state=DoFn.StateParam(VALUES_STATE),
      end_of_window_timer=DoFn.TimerParam(END_OF_WINDOW_TIMER)):
    logging.info('start process.')
    key, value = element
    end_of_window_timer.set(window.end)
    values_state.add(value)
    logging.info('end process.')

  @on_timer(END_OF_WINDOW_TIMER)
  def end_of_window(self, values_state=DoFn.StateParam(VALUES_STATE)):
    logging.info('start end_of_window.')

    read_count = 0
    read_bytes = 0
    values = values_state.read()
    for value in values:
      read_count += 1
      read_bytes += len(value)

    logging.info('read_count: %s, read_bytes: %s', read_count, read_bytes)
    logging.info('end end_of_window.')


def main():
  options = PipelineOptions()
  options.view_as(SetupOptions).save_main_session = True

  p = Pipeline(options=options)
  (p
   | Create(list(range(NUM_SHARDS)))
   | FlatMap(lambda _:
             (bytes(ELEMENT_BYTES) for _ in range(NUM_ELEMENTS_PER_SHARD)))
   | WithKeys('')
   | ParDo(BigBagDoFn()))

  p.run()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  main()
