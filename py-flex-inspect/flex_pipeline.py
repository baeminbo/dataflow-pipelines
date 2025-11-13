import logging
import time

from apache_beam.options.pipeline_options import PipelineOptions

SLEEP_TIME_SECS = 10

def main():
  options = PipelineOptions()
  logging.info("options: %s", options)

  i = 0
  while True:
    logging.info("loop: %s", i)
    time.sleep(SLEEP_TIME_SECS)


if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  main()