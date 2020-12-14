import json

from kafka import KafkaProducer
from kafka.errors import KafkaError

def get_producer(broker_host):
    return KafkaProducer(bootstrap_servers=[broker_host])

def send(broker_host, topic, msg):
    producer = get_producer(broker_host)
    result = producer.send(topic, msg.encode('utf-8'))
    try:
        record_metadata = result.get(timeout=10)
    except KafkaError:
        log.exception()
    pass

def push_metrics(broker_host, topic, metric):
    jd = json.dumps(metric)
    send(broker_host, topic, jd)
    pass