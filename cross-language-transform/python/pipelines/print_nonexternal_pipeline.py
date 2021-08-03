import logging

from apache_beam import Create
from apache_beam import Map
from apache_beam import Pipeline
from apache_beam.options.pipeline_options import PipelineOptions

def main(argv=None):
  options = PipelineOptions(argv)

  p = Pipeline(options=options)

  (p
   | Create(["a", "b", "c", "d", "e"], reshuffle=False)
   | Map(lambda x: logging.info('Print non-external: %s', x)))

  p.run()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  main()
