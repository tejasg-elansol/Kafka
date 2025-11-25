# producer_kafka.py
import json
import cv2
import base64
from kafka import KafkaProducer
from datetime import datetime
from custom_partitioner import custom_safety_partitioner

class SafetyProducer:
    def __init__(self, bootstrap_servers=['192.168.0.56:9092']):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            partitioner=custom_safety_partitioner
        )

        self.topics = {
            "PPE": "safety.ppe",
            "FIRE": "safety.fire",
            "FALL": "safety.fall",
            "VEHICLE": "safety.vehicle",
            "EXIT": "safety.exit",
            "CROWD": "safety.crowd"
        }

    def encode_frame(self, frame):
        resized = cv2.resize(frame, (320, 320))
        ok, buffer = cv2.imencode(".jpg", resized)
        if not ok:
            return None
        return base64.b64encode(buffer).decode('utf-8')

    def send(self, topic_key, key, value):
        """Send a fully formed message"""
        topic = self.topics[topic_key]
        try:
            fut = self.producer.send(topic, key=key.encode(), value=value)
            meta = fut.get(timeout=5)
            print(f"[Kafka] Sent to {topic} | Partition={meta.partition} Offset={meta.offset}")
        except Exception as e:
            print("[Kafka ERROR]", e)

    def close(self):
        self.producer.flush()
        self.producer.close()
        print("[Kafka] Producer closed.")
