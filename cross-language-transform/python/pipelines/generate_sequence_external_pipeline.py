from __future__ import absolute_import

import logging

from apache_beam import BeamJarExpansionService
from apache_beam import Map
from apache_beam import Pipeline
from apache_beam.io.external.generate_sequence import GenerateSequence
from apache_beam.options.pipeline_options import PipelineOptions

# https://stackoverflow.com/a/28052583: Uncomment the following if downloading
# Beam expansion-service artifact download fails with "urlopen error
# [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed"
# ==== WORKAROUND BEGIN ====
import ssl
ssl._create_default_https_context = ssl._create_unverified_context
# ==== WORKAROUND END ====

BEAM_IO_EXPANSION_SERVICE = BeamJarExpansionService(
    'sdks:java:io:expansion-service:shadowJar')

def main(argv=None):
  options = PipelineOptions(argv)
  p = Pipeline(options=options)

  (p
   | GenerateSequence(0, stop=100, expansion_service=BEAM_IO_EXPANSION_SERVICE)
   | Map(lambda x: logging.info(x)))

  p.run()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  main()
