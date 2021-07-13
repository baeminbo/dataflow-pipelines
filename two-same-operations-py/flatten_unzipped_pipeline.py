import logging
from typing import Any
from typing import Tuple

import apache_beam as beam
from apache_beam import Pipeline
from apache_beam import TaggedOutput
from apache_beam.coders import VarIntCoder
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec


class MultiOutputDoFn(beam.DoFn):
  OUTPUT_TAG_A = 'withkey-a'  # main output
  OUTPUT_TAG_B = 'withkey-b'

  def process(self, element):
    yield 'a', element
    yield TaggedOutput(MultiOutputDoFn.OUTPUT_TAG_B, ('b', element))


class StatefulPrintDoFn(beam.DoFn):
  COUNTER_SPEC = ReadModifyWriteStateSpec('counter', VarIntCoder())

  def __init__(self, step_name):
    self._step_name = step_name;

  def process(self, element, counter=beam.DoFn.StateParam(COUNTER_SPEC)):
    current_count = counter.read() or 0
    logging.info('Print [%s] (counter:%d): %s', self._step_name, current_count, element)
    counter.write(current_count + 1)


def main(argv=None):
  options = PipelineOptions(argv)
  options.view_as(SetupOptions).save_main_session = True

  p = Pipeline(options=options)

  input1 = p | 'Input1' >> beam.Create([1, 2, 3], reshuffle=False)
  input2 = p | 'Input2' >> beam.Create([4, 5, 6], reshuffle=False)

  output_a, output_b = (
      (input1, input2)
      | 'Flatten' >> beam.Flatten()
      | 'Split' >> beam.ParDo(MultiOutputDoFn())
      .with_outputs(MultiOutputDoFn.OUTPUT_TAG_B,
                    main=MultiOutputDoFn.OUTPUT_TAG_A))

  # IdentityA and IdentityB are to set output types and set right coders for
  # Dataflow Runner. You may see type inference error (BEAM-4132) without them.

  (output_a
   | 'IdentityA' >> beam.Map(lambda x: x).with_output_types(Tuple[str, int])
   | 'PrintA' >> beam.ParDo(StatefulPrintDoFn('PrintA')))

  (output_b
   | 'IdentityB' >> beam.Map(lambda x: x).with_output_types(Tuple[str, int])
   | 'PrintB' >> beam.ParDo(StatefulPrintDoFn('PrintB')))

  p.run()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  main()