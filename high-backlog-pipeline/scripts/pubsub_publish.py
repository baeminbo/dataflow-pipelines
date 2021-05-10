import argparse
import datetime
import logging
import os
import time
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.futures import Future
from typing import List
from typing import Tuple


def wait_results(futures):
  # type: (List[Tuple[int, Future]]) -> None
  for i, f in futures:
    message_id = f.result()
    logging.info('published. index: %s message_id: %s', i, message_id)


def run(topic, count, payload_size, batch_size):
  client = pubsub_v1.PublisherClient()
  payload = bytes([0 for _ in range(payload_size)])

  futures = []  # type: List[Tuple[int, Future]]
  for i in range(count):
    ts = datetime.datetime.utcnow().isoformat('T') + 'Z'
    future = client.publish(topic, payload, ts=ts)
    futures.append((i, future))

    if len(futures) >= batch_size:
      wait_results(futures)
      futures = []

  wait_results(futures)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)

  parser = argparse.ArgumentParser(
      description='Publish Pub/Sub messages for Dataflow test')
  parser.add_argument('--topic',
                      help='Pub/Sub topic (projects/<PROJECT_ID>/topcis/<TOPIC_ID>',
                      required=True)
  parser.add_argument('--count',
                      help='the number of Pub/Sub messages to publish (default: 4000000)',
                      default=4 * 1000 * 1000)
  parser.add_argument('--payload_size',
                      help='Pub/Sub payload size per message (default: 1KB)',
                      default=1024)
  parser.add_argument('--batch_size',
                      help='batch szie to call Pub/Sub publish API (default: 100000)',
                      default=10000)
  args = parser.parse_args()

  run(args.topic, args.count, args.payload_size, args.batch_size)
