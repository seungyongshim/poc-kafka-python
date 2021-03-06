from kafka import KafkaProducer, KafkaConsumer
import json


def test_simple_publish():
    topic = "com.nexon.ibms.message.test1"
    consumer_group_id = "test1"
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers="localhost:9092",
        group_id=consumer_group_id,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    producer.send(topic, {"test": "something"}).get(timeout=10)

    msg = next(consumer)

    assert {"test": "something"} == msg.value
