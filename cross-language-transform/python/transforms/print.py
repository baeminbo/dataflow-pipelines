import typing

from apache_beam import ExternalTransform
from apache_beam import JavaJarExpansionService
from apache_beam import NamedTupleBasedPayloadBuilder

PrintSchema = typing.NamedTuple('PrintSchema', [('tag', str)])


class Print(ExternalTransform):
  URN = 'beam:external:java:baeminbo:print:v1'

  def __init__(self, tag, expansion_service):
    # type: (str, JavaJarExpansionService) -> None
    super(Print, self).__init__(
        self.URN,
        NamedTupleBasedPayloadBuilder(PrintSchema(tag=tag)),
        expansion_service)
