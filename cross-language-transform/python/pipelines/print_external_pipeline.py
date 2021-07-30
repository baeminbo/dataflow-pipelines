from __future__ import absolute_import

import logging

from apache_beam import Create
from apache_beam import Pipeline
from apache_beam.options.pipeline_options import PipelineOptions

from options import expansion_service
from transforms import Print


def main(argv=None):
  options = PipelineOptions(argv)

  p = Pipeline(options=options)

  (p
   | Create(["a", "b", "c", "d", "e"], reshuffle=False)
   | Print("hello", expansion_service(options)))

  p.run()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  main()
