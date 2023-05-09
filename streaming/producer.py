from concurrent.futures import ThreadPoolExecutor
from typing import List
from kafka import KafkaProducer
import json
import time
from datetime import datetime
import os

producer = KafkaProducer(bootstrap_servers=[os.getenv("KAFKA_BROKER", 'localhost:9092')],
                         value_serializer=lambda m: json.dumps(m).encode('ascii'))


def simulate_car(car_id: int, route: List[dict]):
    for message in route:
        message['car_id'] = car_id
        message['timestamp'] = datetime.now().isoformat()
        print(f"sending data {car_id}: {message}")
        producer.send('first_kafka_topic', message)
        time.sleep(2)


with open("./route.json") as routeb:
    route_with_break = json.loads(routeb.read())
with ThreadPoolExecutor() as executor:
    for i in range(15):
        executor.submit(simulate_car, route=route_with_break, car_id=i)
        time.sleep(3)


