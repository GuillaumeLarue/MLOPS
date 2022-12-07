import json
import random
import time

from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda m: m.encode('utf-8'))
topic = "humidity"


def get_humidity():
    # Get random temperature
    random_humidity = random.randint(0, 100)
    return random_humidity


while True:
    # Sleep between iterations
    rnd_time = random.randint(1, 5)
    time.sleep(rnd_time)

    # Get information
    rnd_id = random.randint(1, 10000)
    rnd_tmp = get_humidity()
    current_date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    # Create python object
    message = {"id": rnd_id, "time": current_date, "temp": rnd_tmp}

    # Convert from Python to JSON
    y = json.dumps(message, indent=4)

    producer.send(topic, y)
    producer.flush()
