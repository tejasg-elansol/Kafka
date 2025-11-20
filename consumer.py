# !/usr/bin/env python3
import json
from kafka import KafkaConsumer

def main():
    # Kafka Consumer configuration
    consumer = KafkaConsumer(
        # Subscribe to all your safety topics
        'safety.ppe',
        'safety.fire',
        'safety.fall',
        'safety.vehicle',
        'safety.exit',
        'safety.crowd',
        
        # Kafka broker LAN IP and port
        bootstrap_servers=['192.168.0.56:9092'],
        
        # Deserialize JSON messages
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        
        # Start reading from the beginning of the topic if no previous offset
        auto_offset_reset='earliest',
        
        # Commit offsets automatically
        enable_auto_commit=True,
        group_id='safety_consumer_group'
    )
    
    print("Kafka Consumer started. Waiting for messages...\n")
    
    try:
        for message in consumer:
            topic = message.topic
            key = message.key.decode('utf-8') if message.key else None
            value = message.value
            
            print(f"Topic: {topic}")
            if key:
                print(f"   Key: {key}")
            print(f"   Value: {json.dumps(value, indent=4)}\n")
    
    except KeyboardInterrupt:
        print("\nConsumer stopped by user.")
    
    finally:
        consumer.close()
        print("Kafka Consumer closed.")

if __name__ == "__main__":
    main()
