from pykafka import KafkaClient
from glob import glob
import queue

# SETUP
client = KafkaClient(zookeeper_hosts = 'localhost:2181')
topic = client.topics['datafeed']
producer = topic.get_producer(
  max_queued_messages = 1000,
  min_queued_messages = 1000,
  linger_ms = 5000,
  queue_empty_timeout_ms = 0,
  block_on_queue_full = True,
  sync = False,
  delivery_reports = True,
  pending_timeout_ms = 5000,
  auto_start = True
)

# PRODUCE DATAFEED
def flush_delivery_reports():
  while True:
    try:
      msg, exc = producer.get_delivery_report(block = False)
      if exc is not None:
        producer.produce(msg)
    except queue.Empty:
      break

fps = sorted(glob('Data/Turnstile/*.txt'))
for fp in fps:
  count = 0
  with open(fp) as fd:
    line = fd.readline() # consume header row
    line = fd.readline()
    while line:
      count += 1
      producer.produce(bytes(line, 'utf-8'))
      line = fd.readline()
      if count % 5000 == 0:
        flush_delivery_reports()
    flush_delivery_reports()
