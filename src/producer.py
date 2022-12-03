import json

from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda m: m.encode('utf-8'),

                        )
topic = "foobar"

producer.send(topic, "toto")
producer.flush()
