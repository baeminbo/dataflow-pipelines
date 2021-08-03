import logging

from apache_beam import Map
from apache_beam import Pipeline
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.options.pipeline_options import PipelineOptions


class KafkaReadOptions(PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument('--topic', type=str, help='Kafka topic to read')
    parser.add_argument('--bootstrap_servers',
                        type=str,
                        help='Bootstrap servers. For example, '
                             '"hostname1:port1,hostname2:port2,'
                             'hostname3:port3"')


def main(argv=None):
  options = PipelineOptions(argv)
  kafka_options = options.view_as(KafkaReadOptions)

  p = Pipeline(options=options)
  (p
   | ReadFromKafka(
          consumer_config={
              'bootstrap.servers': kafka_options.bootstrap_servers},
          topics=[kafka_options.topic])
   | Map(lambda x: logging.info('kafka element: %s', x)))

  p.run()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  main()
