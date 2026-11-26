from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
                        bootstrap_servers='localhost:9092',
                        value_serializer=lambda v: json.dumps(v).encode('utf-8'))

users = ["Alice", "Bob", "Charlie", "David", "Eve"]
pages = ["/home", "/about", "/contact", "/products", "/services"]

try:
    while True:
        event  ={
            "user": random.choice(users),
            "page": random.choice(pages),
            "action": random.choice(["click", "view", "purchase"]),
            "timestamp": int(time.time() * 1000)  # Convert to milliseconds
        }
        producer.send("user-events", value=event)
        print(f"Sent event: {event}")
        time.sleep(0.5)  # Sleep for 1 second before sending the next event

except KeyboardInterrupt:
    print("Stopping producer...")
    
finally:
    producer.flush()
    producer.close()