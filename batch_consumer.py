# VISION CONSUMER

import json
import base64
import numpy as np
import cv2
from kafka import KafkaConsumer
from ppe_class_new import PPE 
from vision_producer import VisionProducer

pobj = VisionProducer()

# -------------------------------
# Create PPE class object
# -------------------------------
ppe = PPE(model_path="C:\VS CODE Folder\Kafka\ppe.pt",org_id="ORG_123")
ppe.load_model()    # loads YOLO inside the class, NOT inside consumer

# -------------------------------
# Kafka Config
# -------------------------------
consumer = KafkaConsumer(
    "raw_frames",
    bootstrap_servers=["192.168.0.56:9092"],
    group_id="ppe-batch-consumer1",
    value_deserializer=lambda v: json.loads(v.decode()),
    key_deserializer=lambda k: k.decode() if k else None,
)

def decode_image(b64_str):
    img_bytes = base64.b64decode(b64_str)
    arr = np.frombuffer(img_bytes, dtype=np.uint8)
    img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
    return img


print("[INFO] Consumer started, waiting for batches...\n")

# -------------------------------
# Main Loop
# -------------------------------
for msg in consumer:

    batch = msg.value
    camera_id = batch["camera_id"]
    frames = batch["frames"]

    # Convert each frame to PPE input format
    ppe_frames = []
    for f in frames:
        img = decode_image(f["image_base64"])
        ppe_frames.append({
            "camera_id": camera_id,
            "frame": img,
            "frame_id": f["frame_id"],
            "timestamp": f["timestamp"],
        })

    print(f"[INFO] Sending batch ({len(frames)} frames) to PPE model…")

    # -------------------------------
    # RUN YOLO INSIDE PPE CLASS
    # -------------------------------
    detections = ppe.batch_infer(ppe_frames)
    ppe.forward_to_producer(pobj, detections)

    print(f"[PPE] Processed {len(detections)} frames\n")
