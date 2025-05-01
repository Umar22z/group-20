import time
import random
import json
from kafka import KafkaProducer

# pip install kafka-python

TOPIC = "traffic_data"
try:
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("Kafka Producer connected successfully!")
except Exception as e:
    print(f"Error connecting to Kafka: {e}")
    exit(1) 

# ///////5 sensors/////////////
sensor_id = ['S101', 'S102', 'S103', 'S104', 'S105']  

while True:
    # Generate random traffic data
    event = {
        "sensor_id": random.choice(sensor_id),
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),  
        "vehicle_count": random.randint(0, 50),  #///////random no.of vehichles////
        "average_speed": round(random.uniform(10.0, 80.0), 2),  
        "congestion_level": random.choice(["LOW", "MEDIUM", "HIGH"])  #///////random congestion level//////
    }

    #/////sending data to kafta////////
    try:
        producer.send(TOPIC, event)
        print(f"Sent event: {event}")  
    except Exception as e:
        print(f"Error sending event: {e}")

    
    time.sleep(1)  #/////1sce wait///
