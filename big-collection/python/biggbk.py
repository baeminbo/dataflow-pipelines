import logging
from typing import Iterable
from typing import Tuple

from apache_beam import Create
from apache_beam import FlatMap
from apache_beam import GroupByKey
from apache_beam import ParDo
from apache_beam import Pipeline
from apache_beam import WithKeys
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

# The total bytes processed is NUM_SHARDS * NUM_ELEMENTS_PER_SHARD * ELEMENT_BYTES ~= 3 GiB

NUM_SHARDS = 100
NUM_ELEMENTS_PER_SHARD = 10
ELEMENT_BYTES = 3 * 1024 * 1024  # 3 MiB

def print_bytes(element: Tuple[str, Iterable[bytes]]) -> None:
  logging.info('start print_bytes.')
  key, values = element

  read_count = 0
  read_bytes = 0
  for value in values:
    read_count += 1
    read_bytes += len(value)

  logging.info('read_count: %s, read_bytes: %s', read_count, read_bytes)
  logging.info('end print_bytes.')


def main():
  options = PipelineOptions()
  options.view_as(SetupOptions).save_main_session = True

  p = Pipeline(options=options)
  (p
   | Create(list(range(NUM_SHARDS)))
   | FlatMap(lambda _:
             (bytes(ELEMENT_BYTES) for _ in range(NUM_ELEMENTS_PER_SHARD)))
   | WithKeys('')
   | GroupByKey()
   | ParDo(print_bytes))

  p.run()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  main()
