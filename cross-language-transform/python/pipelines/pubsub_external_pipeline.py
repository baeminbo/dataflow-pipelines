from __future__ import absolute_import

import logging

from apache_beam import Map
from apache_beam import Pipeline
from apache_beam.io import PubsubMessage
from apache_beam.io.external.gcp.pubsub import ReadFromPubSub
from apache_beam.options.pipeline_options import PipelineOptions

from options import expansion_service


class PubSubTopicOptions(PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument('--topic',
                        type=str,
                        help='Pub/Sub topic (projects/<PROJECT_ID>/topics'
                             '/<TOPIC_ID>')


def main(argv=None):
  options = PipelineOptions(argv)
  topic = options.view_as(PubSubTopicOptions).topic

  p = Pipeline(options=options)
  (p
   # This is an external transform
   # `apache_beam.io.external.gcp.pubsub.ReadFromPubSub`. This is different from
   # `apache_beam.io.gcp.pubsub.ReadFromPubSub` which is native transform used
   # for most cases.
   #
   # If you set expansion_service as BeamJarExpansionService(
   # 'sdks:java:io:google-cloud-platform:expansion-service:shadowJar'), it will
   # fail as the beam jar has no dependency for DirectRunner. As a workaround,
   # specify custom expansion service jar in this project.
   | ReadFromPubSub(topic=topic,
                    with_attributes=True,
                    expansion_service=expansion_service(options))
   | Map(lambda message: logging.info("message: %s", message))
   )
  p.run()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  main()
