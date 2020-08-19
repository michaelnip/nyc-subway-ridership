from pykafka import KafkaClient
from pykafka.common import OffsetType
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
import threading

# CONSUMER THREAD
def consume(lock):
  consumer = topic.get_balanced_consumer(
    consumer_group = b'consumers',
    auto_commit_enable = True,
    auto_commit_interval_ms = 60000,
    auto_offset_reset = OffsetType.EARLIEST,
    consumer_timeout_ms = 5000,
    zookeeper_connect = 'localhost:2181',
    auto_start = True,
    reset_offset_on_start = False
  )

  count = 0
  msg = consumer.consume()
  while msg is not None:
    row = str(msg.value, 'utf-8')
    if count == 0:
      df = spark.createDataFrame([(row)], StringType())
    else:
      tmp = spark.createDataFrame([(row)], StringType())
      df = df.union(tmp)
    count += 1
    if count >= 50000:
      lock.acquire()
      df.write.mode('append').save('datafeed_parts', format = 'text')
      lock.release()
      count = 0
    msg = consumer.consume()

  if count > 0:
    lock.acquire()
    df.write.mode('append').save('datafeed_parts', format = 'text')
    lock.release()

# MAIN THREAD
if __name__ == '__main__':
  client = KafkaClient(zookeeper_hosts = 'localhost:2181')
  topic = client.topics['datafeed']
  spark = SparkSession.builder.appName('consumer').getOrCreate()

  partitions = 10
  lock = threading.Lock()
  threads = []
  for i in range(partitions):
    thread = threading.Thread(target = consume, args = (lock,))
    threads.append(thread)
  for thread in threads:
    thread.start()
  for thread in threads:
    thread.join()

  spark.stop()
