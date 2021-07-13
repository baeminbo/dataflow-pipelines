import logging

import apache_beam as beam
from apache_beam import Pipeline
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.pvalue import AsList


def main(argv=None):
  options = PipelineOptions(argv)
  p = Pipeline(options = options)

  input = p | 'Input' >> beam.Create([1, 2, 3], reshuffle=False)
  output1 = input | 'Output1' >> beam.Map(lambda x, side: (x, side),
                                          AsList(input))
  input | 'Output2' >> beam.Map(
    lambda x, side: logging.info('x: %s, side: %s', x, side), AsList(output1))

  p.run()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  main()