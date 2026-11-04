import json
import logging
import os
import signal
from confluent_kafka import Consumer, KafkaError

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
NAMESPACE = os.environ['EVENTHUB_NAMESPACE'].removesuffix('.servicebus.windows.net')
CONNECTION = os.environ['EVENTHUB_CONNECTION_STRING']
HUBS = ['commerce-events', 'supply-events', 'people-events', 'finance-events']
GROUP = os.getenv('KAFKA_CONSUMER_GROUP', 'nilemart-learning-consumers')
stop = False

def terminate(*_):
    global stop
    stop = True
signal.signal(signal.SIGTERM, terminate)
signal.signal(signal.SIGINT, terminate)

consumer = Consumer({
    'bootstrap.servers': f'{NAMESPACE}.servicebus.windows.net:9093',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': '$ConnectionString',
    'sasl.password': CONNECTION,
    'group.id': GROUP,
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
})
consumer.subscribe(HUBS)
logging.info('Consumer group=%s subscribed to %s', GROUP, ', '.join(HUBS))
try:
    while not stop:
        msg = consumer.poll(1)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                logging.error('Read error: %s', msg.error())
            continue
        try:
            record = json.loads(msg.value())
            logging.info('RECEIVED hub=%s partition=%s offset=%s type=%s id=%s',
                msg.topic(), msg.partition(), msg.offset(), record.get('event_type'), record.get('event_id'))
            # In this lab the durable output is the container logs only.
            # Later replace this with a destination write BEFORE committing offsets.
            consumer.commit(message=msg, asynchronous=False)
        except Exception:
            logging.exception('Processing failed; offset not committed')
finally:
    consumer.close()
