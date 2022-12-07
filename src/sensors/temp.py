import json
import random
import time

from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda m: m.encode('utf-8'))
topic = "temperature"


def get_temperature():
    # Get random temperature
    random_temperature = random.randint(0, 1000)
    return random_temperature


while True:
    # Sleep between iterations
    rnd_time = random.randint(1, 5)
    time.sleep(rnd_time)

    # Get information
    rnd_id = random.randint(1, 10000)
    rnd_tmp = get_temperature()
    current_date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    # Create python object
    message = {"id": rnd_id, "time": current_date, "temp": rnd_tmp}

    # Convert from Python to JSON
    y = json.dumps(message, indent=4)

    producer.send(topic, y)
    producer.flush()
