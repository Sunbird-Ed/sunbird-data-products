from kafka import KafkaProducer
from kafka.errors import KafkaError
import json

#producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

def get_producer(broker_host):
    KafkaProducer(bootstrap_servers=[broker_host])

def push_metrics(broker_host, topic, metric):
    jd = json.dumps(metric)
    result = get_producer(broker_host).send(topic, jd.encode('utf-8'))
    try:
        record_metadata = result.get(timeout=10)
    except KafkaError:
        log.exception()
    pass