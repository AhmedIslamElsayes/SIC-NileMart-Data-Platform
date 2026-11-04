import json
import logging
import os
import random
import signal
import time
import uuid
from datetime import datetime, timezone
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
NAMESPACE = os.getenv('EVENTHUB_NAMESPACE', '').removesuffix('.servicebus.windows.net')
CONNECTION = os.getenv('EVENTHUB_CONNECTION_STRING', '')
if not NAMESPACE or not CONNECTION:
    logging.warning('Waiting for Azure Container App configuration: EVENTHUB_NAMESPACE and EVENTHUB_CONNECTION_STRING are required')
    while True:
        time.sleep(300)
INTERVAL = max(float(os.getenv('EVENT_INTERVAL_SECONDS', '5')), 0.1)
HUBS = ['commerce-events', 'supply-events', 'people-events', 'finance-events']
stop = False

def terminate(*_):
    global stop
    stop = True

signal.signal(signal.SIGTERM, terminate)
signal.signal(signal.SIGINT, terminate)

producer = Producer({
    'bootstrap.servers': f'{NAMESPACE}.servicebus.windows.net:9093',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': '$ConnectionString',
    'sasl.password': CONNECTION,
    'client.id': 'nilemart-cloud-generator',
})

def report(error, msg):
    if error:
        logging.error('Delivery failed: %s', error)
    else:
        logging.info('SENT hub=%s partition=%s offset=%s', msg.topic(), msg.partition(), msg.offset())

counter = 0
logging.info('Starting simulated domain events (no Azure SQL writes).')
while not stop:
    domain_i = counter % len(HUBS)
    topic = HUBS[domain_i]
    entity_id = random.randint(1, 10000)
    domain = topic.removesuffix('-events')
    event = {
        'event_id': str(uuid.uuid4()),
        'event_type': {'commerce': 'OrderCreated', 'supply': 'InventoryAdjusted',
                       'people': 'AttendanceRecorded', 'finance': 'InvoiceIssued'}[domain],
        'domain': domain,
        'source': 'nilemart-cloud-simulator',
        'entity_id': entity_id,
        'event_time': datetime.now(timezone.utc).isoformat(),
        'simulation': True,
        'sample_value': random.randint(1, 1000),
    }
    try:
        producer.produce(topic, key=str(entity_id), value=json.dumps(event).encode(), callback=report)
    except BufferError:
        logging.warning('Producer queue full, retrying')
        producer.poll(1)
        continue
    producer.poll(0)
    counter += 1
    time.sleep(INTERVAL)
logging.info('Stopping, flushing outgoing events')
remaining = producer.flush(30)
if remaining:
    logging.error('%s messages were not delivered', remaining)
