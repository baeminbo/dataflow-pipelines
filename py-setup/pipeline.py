import logging

from apache_beam import Pipeline, Create, ParDo, DoFn
from apache_beam.options.pipeline_options import PipelineOptions

from utils.functions import print_element


class PrintElement(DoFn):
    def process(self, element):
        print_element(element)

def main():
    options = PipelineOptions()
    p = Pipeline(options=options)

    (p | Create(list(range(10)))
     | ParDo(PrintElement()))

    p.run()

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    main()
