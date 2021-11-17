import logging

from apache_beam import Create
from apache_beam import GroupIntoBatches
from apache_beam import Map
from apache_beam import ParDo
from apache_beam import Pipeline
from apache_beam import WithKeys
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


def make_large_elements(input):
  ELEMENT_COUNT = 10
  BUFFER_BYTES = 10 * 1024 * 1024
  logging.info('input %s to %s elements with buf %s bytes',
               input, ELEMENT_COUNT, BUFFER_BYTES)
  for i in range(ELEMENT_COUNT):
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
   | GroupIntoBatches(BATCH_SIZE, BUFFERING_SECS)  # Big batch size with 1 minute trigger
   | Map(lambda kv: logging.info('key: %s, value count: %s',
                                 kv[0], len(kv[1]))))

  run = p.run()
  run.wait_until_finish()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  main()
