import json
from kafka import KafkaProducer


class VisionProducer:
    def __init__(self, bootstrap_servers=None):
        if bootstrap_servers is None:
            bootstrap_servers = ["192.168.0.56:9092"]

        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

        # Topic mapping for all use-cases
        self.topics = {
            "PPE": "safety.ppe",
            "FIRE": "safety.fire",
            "FALL": "safety.fall",
            "VEHICLE": "safety.vehicle",
            "EXIT": "safety.exit",
            "CROWD": "safety.crowd",
        }

    def send(self, use_case, key, message_dict):
        """
        Sends a READY Kafka message (already formatted by the model class).
        """
        topic = self.topics.get(use_case)
        if topic is None:
            print(f"[ERROR] Unknown use case: {use_case}")
            return

        try:
            self.producer.send(
                topic,
                key=str(key).encode("utf-8"),
                value=message_dict
            )
            self.producer.flush()
        except Exception as e:
            print(f"[ERROR] Failed to send message: {e}")
