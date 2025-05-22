import logging
from typing import Any, Iterable, Tuple

from apache_beam import Pipeline, Impulse, ParDo, GroupIntoBatches
from apache_beam.options.pipeline_options import PipelineOptions

SHARD_COUNT = 1000
ELEMENT_COUNT = 100_000


def generate(_: Any) -> Iterable[Tuple[int, int]]:
    logging.info(f'Generating {ELEMENT_COUNT} elements with {SHARD_COUNT} shards.')
    yield from ((i % SHARD_COUNT, i) for i in range(ELEMENT_COUNT))


def process(element: Tuple[int, Iterable[int]]) -> None:
    logging.info(f'Processing element {element}')


def main():
    options = PipelineOptions()
    pipeline = Pipeline(options=options)
    (pipeline
     | Impulse()
     | "Generate" >> ParDo(generate)
     | GroupIntoBatches(batch_size=10)
     | "Process" >> ParDo(process))

    pipeline.run()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()
