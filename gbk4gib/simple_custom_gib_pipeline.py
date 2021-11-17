import logging
import time

from apache_beam import Create
from apache_beam import DoFn
from apache_beam import Map
from apache_beam import PTransform
from apache_beam import ParDo
from apache_beam import Pipeline
from apache_beam import TimeDomain
from apache_beam import WithKeys
from apache_beam import coders
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.transforms.combiners import CountCombineFn
from apache_beam.transforms.userstate import BagStateSpec
from apache_beam.transforms.userstate import CombiningValueStateSpec
from apache_beam.transforms.userstate import TimerSpec
from apache_beam.transforms.userstate import on_timer


class GroupIntoBatchesWithMultiBags(PTransform):
  """PTransform that batches the input into desired batch size. Elements are
  buffered until they are equal to batch size provided in the argument at which
  point they are output to the output Pcollection.

  Windows are preserved (batches will contain elements from the same window)

  GroupIntoBatches is experimental. Its use case will depend on the runner if
  it has support of States and Timers.
  """
  def __init__(
      self, batch_size, max_buffering_duration_secs=None, clock=time.time):
    """Create a new GroupIntoBatches.

    Arguments:
      batch_size: (required) How many elements should be in a batch
      max_buffering_duration_secs: (optional) How long in seconds at most an
        incomplete batch of elements is allowed to be buffered in the states.
        The duration must be a positive second duration and should be given as
        an int or float. Setting this parameter to zero effectively means no
        buffering limit.
      clock: (optional) an alternative to time.time (mostly for testing)
    """
    self.batch_size = batch_size
    self.max_buffering_duration_secs = max_buffering_duration_secs
    self.clock = clock

  def expand(self, pcoll):
    input_coder = coders.registry.get_coder(pcoll)
    return pcoll | ParDo(
        _pardo_group_into_batches_with_multi_bags(
            input_coder,
            self.batch_size,
            self.max_buffering_duration_secs,
            self.clock))


def _pardo_group_into_batches_with_multi_bags(
  input_coder, batch_size, max_buffering_duration_secs, clock=time.time):
  ELEMENT_STATE_0 = BagStateSpec('values0', input_coder)
  ELEMENT_STATE_1 = BagStateSpec('values1', input_coder)
  ELEMENT_STATE_2 = BagStateSpec('values2', input_coder)
  ELEMENT_STATE_3 = BagStateSpec('values3', input_coder)
  COUNT_STATE = CombiningValueStateSpec('count', input_coder, CountCombineFn())
  WINDOW_TIMER = TimerSpec('window_end', TimeDomain.WATERMARK)
  BUFFERING_TIMER = TimerSpec('buffering_end', TimeDomain.REAL_TIME)

  class _GroupIntoBatchesDoFnWithMultiBags(DoFn):
    def process(
        self,
        element,
        window=DoFn.WindowParam,
        element_state_0=DoFn.StateParam(ELEMENT_STATE_0),
        element_state_1=DoFn.StateParam(ELEMENT_STATE_1),
        element_state_2=DoFn.StateParam(ELEMENT_STATE_2),
        element_state_3=DoFn.StateParam(ELEMENT_STATE_3),
        count_state=DoFn.StateParam(COUNT_STATE),
        window_timer=DoFn.TimerParam(WINDOW_TIMER),
        buffering_timer=DoFn.TimerParam(BUFFERING_TIMER)):
      # Allowed lateness not supported in Python SDK
      # https://beam.apache.org/documentation/programming-guide/#watermarks-and-late-data
      window_timer.set(window.end)

      count_state.add(1)
      count = count_state.read()

      element_states = [element_state_0, element_state_1, element_state_2, element_state_3]
      element_states[count % 4].add(element)

      if count == 1 and max_buffering_duration_secs > 0:
        # This is the first element in batch. Start counting buffering time if a
        # limit was set.
        buffering_timer.set(clock() + max_buffering_duration_secs)
      if count >= batch_size:
        return self.flush_batch(element_states, count_state, buffering_timer)

    @on_timer(WINDOW_TIMER)
    def on_window_timer(
        self,
        element_state_0=DoFn.StateParam(ELEMENT_STATE_0),
        element_state_1=DoFn.StateParam(ELEMENT_STATE_1),
        element_state_2=DoFn.StateParam(ELEMENT_STATE_2),
        element_state_3=DoFn.StateParam(ELEMENT_STATE_3),
        count_state=DoFn.StateParam(COUNT_STATE),
        buffering_timer=DoFn.TimerParam(BUFFERING_TIMER)):

      element_states = [element_state_0, element_state_1, element_state_2, element_state_3]
      return self.flush_batch(element_states, count_state, buffering_timer)

    @on_timer(BUFFERING_TIMER)
    def on_buffering_timer(
        self,
        element_state_0=DoFn.StateParam(ELEMENT_STATE_0),
        element_state_1=DoFn.StateParam(ELEMENT_STATE_1),
        element_state_2=DoFn.StateParam(ELEMENT_STATE_2),
        element_state_3=DoFn.StateParam(ELEMENT_STATE_3),
        count_state=DoFn.StateParam(COUNT_STATE),
        buffering_timer=DoFn.TimerParam(BUFFERING_TIMER)):

      element_states = [element_state_0, element_state_1, element_state_2, element_state_3]
      return self.flush_batch(element_states, count_state, buffering_timer)

    def flush_batch(self, element_states, count_state, buffering_timer):
      batch_values = []
      for element_state in element_states:
        for k, v in element_state.read():
          key = k
          batch_values.append(v)
        element_state.clear()

      count_state.clear()
      buffering_timer.clear()

      if not batch_values:
        return

      yield key, batch_values

  return _GroupIntoBatchesDoFnWithMultiBags()


def make_large_elements(input):
  ELEMENT_COUNT = 10
  BUFFER_BYTES = 10 * 1024 * 1024
  logging.info('input %s to %s elements with buf %s bytes',
               input, ELEMENT_COUNT, BUFFER_BYTES)
  for i in range(ELEMENT_COUNT):
    # Use random bytes to avoid size reduced by compression possibly
    buf = bytearray(BUFFER_BYTES)
    yield input, i, buf


def main():
  options = PipelineOptions()
  options.view_as(SetupOptions).save_main_session = True

  BATCH_SIZE = 1000000
  BUFFERING_SECS = 600

  p = Pipeline(options=options)
  (p
   | Create(range(100), reshuffle=True)
   | ParDo(make_large_elements)  # 128 KiB
   | WithKeys('')
   | GroupIntoBatchesWithMultiBags(BATCH_SIZE, BUFFERING_SECS)  # Big batch size with 1 minute trigger
   | Map(lambda kv: logging.info('key: %s, value count: %s',
                                 kv[0], len(kv[1]))))

  run = p.run()
  run.wait_until_finish()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  main()
