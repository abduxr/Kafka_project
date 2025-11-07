from fastapi import FastAPI, Request
from confluent_kafka import Producer
from pymongo import MongoClient
import json

app = FastAPI()

# Kafka Producer Config
conf = {
    'bootstrap.servers': 'localhost:9092',   # change if using remote brokerr
}
producer = Producer(conf)

# MongoDB Connection
mongo_client = MongoClient("mongodb://localhost:27017")
db = mongo_client["customer_events_db"]
collection = db["touch_events"]

# Helper function to send to Kafka
def send_to_kafka(topic, data):
    producer.produce(topic, json.dumps(data).encode('utf-8'))
    producer.flush()

@app.post("/api/event")
async def receive_event(request: Request):
    body = await request.json()
    event_data = {
        "customer_id": body.get("customer_id"),
        "event_type": body.get("event_type"),
        "timestamp": body.get("timestamp"),
        "metadata": body.get("metadata")
    }

    # 1️⃣ Store event in MongoDB
    collection.insert_one(event_data)

    # 2️⃣ Send event to Kafka
    send_to_kafka("customer-events", event_data)
    print("hi,hello")
    return {"status": "success", "message": "Event processed"} 
