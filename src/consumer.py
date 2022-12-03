import json

from kafka import KafkaConsumer

topic = "foobar"
consumer = KafkaConsumer(topic, bootstrap_servers=['localhost:9092'],
                         value_deserializer=lambda m: m.decode('utf-8'))

for message in consumer:  # boucle infinie
    print(type(message.value))
    print(message.value)
