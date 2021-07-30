from apache_beam import JavaJarExpansionService
from apache_beam.options.pipeline_options import PipelineOptions


class JavaJarExpansionServiceOptions(PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument('--expansion_service',
                        type=str,
                        help='JAR path or service endpoint (<hostname>:<port>)')


def expansion_service(options):
  # type: (PipelineOptions) -> JavaJarExpansionService
  return JavaJarExpansionService(
      options.view_as(JavaJarExpansionServiceOptions).expansion_service)
