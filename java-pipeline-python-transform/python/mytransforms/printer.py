import logging

from apache_beam import DoFn
from apache_beam import PCollection
from apache_beam import PTransform
from apache_beam import ParDo
from apache_beam import typehints

# Maybe should use a primitive type for output (e.g. int, bytes, str)
@typehints.with_output_types(str)
class Printer(PTransform):
  def __init__(self, name):
    # type: (str) -> None
    self._name = name

  def expand(self, input):
    # type: (PCollection[Long]) -> PCollection[str]
    return input.apply(ParDo(PrinterDoFn(self._name)))


class PrinterDoFn(DoFn):
  def __init__(self, name):
    self._name = name

  def process(self, element):
    logging.info(f"[{self._name}]: {element}")
