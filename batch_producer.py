import time
import uuid
import base64
import json
import cv2
from kafka import KafkaProducer
from videoCapture import VideoCapture   # your existing capture class

# ---- CAMERA URLs ----
CAMERAS = {
    "cam1": "rtsp://admin:Elansol%402020@192.168.0.92:554/Streaming/Channels/102",
    "cam2": "rtsp://admin:Elansol%402020@192.168.0.93:554/Streaming/Channels/102",
    "cam3": "rtsp://admin:Elansol%402020@192.168.0.94:554/Streaming/Channels/102"
}

BATCH_SIZE = 8
KAFKA_TOPIC = "raw_frames"
KAFKA_BROKER = "192.168.0.56:9092"

#  Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8") if k else None,
    linger_ms=20,
    batch_size=64 * 1024,   # 64KB
    acks="all"
)

#   Convert OpenCV frame → JPEG → Base64 for JSON
def encode_frame_to_base64(frame):
    success, buffer = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, 80])
    if not success:
        return None
    return base64.b64encode(buffer).decode("utf-8")

#   Send one batch to Kafka
def send_batch_to_kafka(camera_id, batch_frames):

    batch_id = str(uuid.uuid4())
    batch_timestamp = int(time.time() * 1000)

    batch_payload = {
        "camera_id": camera_id,
        "batch_id": batch_id,
        "timestamp": batch_timestamp,
        "frames": []
    }

    for (frame, ts) in batch_frames:
        frame = cv2.resize(frame, (320, 320))
        encoded = encode_frame_to_base64(frame)
        if encoded is None:
            continue

        frame_entry = {
            "frame_id": str(uuid.uuid4()),
            "timestamp": ts,
            "image_base64": encoded
        }

        batch_payload["frames"].append(frame_entry)

    # Send to Kafka
    producer.send(
        KAFKA_TOPIC,
        key=camera_id,
        value=batch_payload
    )
    producer.flush()

    print(f"[KAFKA] Sent batch → cam={camera_id}, batch_id={batch_id}, frames={len(batch_frames)}")

#   Main continuous capture + batch sending
def continuous_batch_capture():

    # initialize VideoCapture objects
    cams = {}
    for cam_id, url in CAMERAS.items():
        print(f"[INFO] Opening {cam_id} → {url}")
        cams[cam_id] = VideoCapture(url, log_name=f"{cam_id}.log", camera_label=cam_id)

    batches = {cam_id: [] for cam_id in CAMERAS}

    print("\n[INFO] Continuous frame capture started...\n")

    while True:
        for cam_id, cam in cams.items():

            frame_data = cam.read()   # returns [frame, timestamp] or None
            if frame_data is None:
                continue

            frame, ts = frame_data
            if frame is None:
                continue

            # Add to batch
            batches[cam_id].append((frame, ts))
            print(f"[{cam_id}] {len(batches[cam_id])}/{BATCH_SIZE}")

            # Batch ready → send to Kafka
            if len(batches[cam_id]) == BATCH_SIZE:

                print(f"\n=== BATCH READY from {cam_id} ({BATCH_SIZE} frames) ===")

                send_batch_to_kafka(cam_id, batches[cam_id])

                # Clear batch
                batches[cam_id] = []

        time.sleep(0.005)  # avoid CPU spike

if __name__ == "__main__":
    continuous_batch_capture()
