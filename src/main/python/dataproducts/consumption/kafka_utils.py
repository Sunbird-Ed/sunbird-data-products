from kafka import KafkaProducer
from kafka.errors import KafkaError
import json

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

def push_metrics(topic, metric):
    jd = json.dumps(metric)
    result = producer.send(topic, jd.encode('utf-8'))
    try:
        record_metadata = result.get(timeout=10)
    except KafkaError:
        log.exception()
    pass