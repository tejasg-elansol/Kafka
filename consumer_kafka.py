from kafka import KafkaConsumer
import json
import base64
import numpy as np
from datetime import datetime
import cv2

# ==============================
# 1. Kafka Consumer Setup
# ==============================
consumer = KafkaConsumer(
    'safety.ppe',
    bootstrap_servers=['192.168.0.56:9092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='ppe-consumer-group-v10',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("🚀 PPE Consumer started... Listening to 'safety.ppe' topic\n")
print("-" * 80)

# ==============================
# 2. Consume & Display Messages
# ==============================
try:
    message_count = 0
    partition_stats = {}
    
    for message in consumer:
        message_count += 1
        
        topic = message.topic
        partition = message.partition
        offset = message.offset
        timestamp = message.timestamp
        value = message.value
        
        # Track partition stats
        partition_stats[partition] = partition_stats.get(partition, 0) + 1

        # ==============================
        # Decode Frame + MEASURE SIZE
        # ==============================
        frame_b64 = value.get("frame")

        if frame_b64:

            try:
                # Decode base64 → raw image bytes
                img_bytes = base64.b64decode(frame_b64)

                # Convert buffer → cv2 frame
                np_arr = np.frombuffer(img_bytes, np.uint8)
                frame_cv2 = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)

                if frame_cv2 is not None:
                    # Show image
                    cv2.imshow("Received Frame", frame_cv2)
                    if cv2.waitKey(1) & 0xFF == ord('q'):
                        break

            except Exception as e:
                print("Frame decode error ❌:", e)
        else:
            print("No frame field ❌")

        # ==============================
        # Print Message Metadata
        # ==============================
        print(f"\n📨 Message #{message_count}")
        print(f"Topic: {topic} | Partition: {partition} | Offset: {offset}")
        print(f"Timestamp: {datetime.fromtimestamp(timestamp/1000).strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Data: {json.dumps(value, indent=2)}")
        print("-" * 80)

        # Stats every 10 messages
        if message_count % 10 == 0:
            print(f"\n📊 Partition Distribution after {message_count} messages:")
            for p, count in sorted(partition_stats.items()):
                print(f"   Partition {p}: {count} messages")
            print("-" * 80)

except KeyboardInterrupt:
    print("\n\n🛑 Consumer stopped manually.")
    print(f"\n📊 Total messages: {message_count}")
    print("\nPartition Distribution:")
    for p, count in sorted(partition_stats.items()):
        print(f"   Partition {p}: {count} messages")

finally:
    consumer.close()
    cv2.destroyAllWindows()
    print("✅ Kafka consumer closed.")
