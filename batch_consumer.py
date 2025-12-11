# # BATCH VISION CONSUMER

# import json
# import base64
# import numpy as np
# import cv2
# from kafka import KafkaConsumer
# from usecases.ppe import PPE 
# from usecases.person_detection import PersonDetection
# from vision_producer import VisionProducer

# pobj = VisionProducer()

# # -------------------------------
# # Create PPE class object
# # -------------------------------
# ppe = PPE(model_path="C:\VS CODE Folder\Kafka\models\ppe.pt",org_id="ORG_123")
# ppe.load_model()    # loads YOLO inside the class, NOT inside consumer

# # Create Person detection class object
# person_detector = PersonDetection(
#     model_path="C:\\VS CODE Folder\\Kafka\\models\\person_detection.pt",
#     use_cases=["CROWD"],     # <-- IMPORTANT
#     org_id="ORG_123"
# )
# person_detector.load_model()

# # -------------------------------
# # Kafka Config
# # -------------------------------
# consumer = KafkaConsumer(
#     "raw_frames",
#     bootstrap_servers=["192.168.0.56:9092"],
#     group_id="vision-batch-consumer3",
#     value_deserializer=lambda v: json.loads(v.decode()),
#     key_deserializer=lambda k: k.decode() if k else None,
# )

# def decode_image(b64_str):
#     img_bytes = base64.b64decode(b64_str)
#     arr = np.frombuffer(img_bytes, dtype=np.uint8)
#     img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
#     return img


# print("[INFO] Consumer started, waiting for batches...\n")

# # -------------------------------
# # Main Loop
# # -------------------------------
# for msg in consumer:

#     batch = msg.value
#     camera_id = batch["camera_id"]
#     frames = batch["frames"]

#     # Convert each frame to PPE input format
#     model_frames = []
#     for f in frames:
#         img = decode_image(f["image_base64"])
#         model_frames.append({
#             "camera_id": camera_id,
#             "frame": img,
#             "frame_id": f["frame_id"],
#             "timestamp": f["timestamp"],
#         })

#     print(f"[INFO] Sending batch ({len(frames)} frames) to PPE model…")

#     # 1️⃣ PPE MODEL
#     # -------------------------------------
#     ppe_out = ppe.batch_infer(model_frames)
#     ppe.forward_to_producer(pobj, ppe_out)
#     print("[PPE] Done.")

#     # -------------------------------------
#     # 2️⃣ PERSON DETECTION MODEL
#     # -------------------------------------
#     person_out = person_detector.batch_infer(model_frames)
#     person_detector.forward_to_producer(pobj, person_out)

# #     print("[PERSON DETECTION] Done.\n")
# import socket
# import struct
# import json
# import base64
# import numpy as np
# import cv2
# import time

# from usecases.ppe import PPE
# from vision_producer import VisionProducer

# HOST = "0.0.0.0"
# PORT = 5005

# # --------------------------------------------------------
# # Initialize PPE model + VisionProducer
# # --------------------------------------------------------
# print("[INFO] Initializing PPE model...")
# ppe = PPE(
#     model_path="C:\VS CODE Folder\Kafka\models\ppe.pt",
#     org_id="ORG_123"
# )
# ppe.load_model()

# pobj = VisionProducer()


# # --------------------------------------------------------
# # Base64 → OpenCV image
# # --------------------------------------------------------
# def decode_image(b64_str):
#     img_bytes = base64.b64decode(b64_str)
#     arr = np.frombuffer(img_bytes, np.uint8)
#     img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
#     return img


# # --------------------------------------------------------
# # Receive length-prefixed JSON message
# # --------------------------------------------------------
# def recv_payload(conn):
#     raw_len = conn.recv(4)
#     if not raw_len:
#         return None

#     msg_len = struct.unpack(">I", raw_len)[0]

#     data = b""
#     while len(data) < msg_len:
#         packet = conn.recv(msg_len - len(data))
#         if not packet:
#             return None
#         data += packet

#     return json.loads(data.decode("utf-8"))


# # --------------------------------------------------------
# # TCP Server → Batch Inference → Kafka
# # --------------------------------------------------------
# def run_server():
#     s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#     s.bind((HOST, PORT))
#     s.listen(5)

#     print(f"[INFO] TCP PPE Batch Inference Server running on {HOST}:{PORT}")

#     while True:
#         conn, addr = s.accept()
#         print(f"\n[INFO] Client connected: {addr}")

#         while True:
#             payload = recv_payload(conn)
#             if payload is None:
#                 print("[WARN] Client disconnected.")
#                 break

#             # -------------------------------
#             # Extract batch
#             # -------------------------------
#             frames_list = payload.get("frames", [])

#             if not frames_list:
#                 print("[WARN] Empty batch received")
#                 conn.sendall(b"EMPTY")
#                 continue

#             print(f"[INFO] Received batch of size: {len(frames_list)}")

#             total_start = time.time()

#             # -------------------------------
#             # Decode ALL frames in batch
#             # -------------------------------
#             ppe_frames = []
#             decode_start = time.time()

#             for f in frames_list:
#                 img = decode_image(f["frameData"])

#                 ppe_frames.append({
#                     "camera_id": f["cameraId"],
#                     "frame_id": f["frameId"],
#                     "timestamp": f["timestamp"],
#                     "frame": img
#                 })

#             decode_end = time.time()

#             # -------------------------------
#             # Run batch inference
#             # -------------------------------
#             infer_start = time.time()
#             detections = ppe.batch_infer(ppe_frames)
#             infer_end = time.time()

#             # -------------------------------
#             # Send results to Kafka
#             # -------------------------------
#             vp_start = time.time()
#             ppe.forward_to_producer(pobj, detections)
#             vp_end = time.time()

#             total_end = time.time()

#             # -------------------------------
#             # Log timing
#             # -------------------------------
#             print("\n========== TCP BATCH → PPE PIPELINE ==========")
#             print(f"Batch size                 : {len(frames_list)}")
#             print(f"Base64 → Image decode time : {decode_end - decode_start:.4f} sec")
#             print(f"PPE YOLO inference time    : {infer_end - infer_start:.4f} sec")
#             print(f"Kafka send time            : {vp_end - vp_start:.4f} sec")
#             print("----------------------------------------------")
#             print(f"TOTAL pipeline time        : {total_end - total_start:.4f} sec")
#             print("==============================================\n")

#             # Acknowledge processing
#             conn.sendall(b"OK")

#         conn.close()


# if __name__ == "__main__":
#     run_server()

import socket
import struct
import json
import base64
import numpy as np
import cv2

from usecases.ppe import PPE
# from usecases.person_detection import PersonDetection
from vision_producer import VisionProducer

HOST = "0.0.0.0"
PORT = 5005


# ----------------------------------------------------------
# Load Models (same as batch_consumer.py)
# ----------------------------------------------------------
print("[INFO] Initializing Vision Models...")

ppe = PPE(
    model_path="C:\\VS CODE Folder\\Kafka\\models\\ppe.pt",
    org_id="ORG_123"
)
ppe.load_model()

# person_detector = PersonDetection(
#     model_path="C:\\VS CODE Folder\\Kafka\\models\\person_detection.pt",
#     use_cases=["CROWD"],
#     org_id="ORG_123"
# )
# person_detector.load_model()

pobj = VisionProducer()


# ----------------------------------------------------------
# Helpers
# ----------------------------------------------------------
def decode_image(b64_str):
    img_bytes = base64.b64decode(b64_str)
    arr = np.frombuffer(img_bytes, dtype=np.uint8)
    return cv2.imdecode(arr, cv2.IMREAD_COLOR)


def recv_payload(conn):
    """Receive length-prefixed JSON payload."""
    raw_len = conn.recv(4)
    if not raw_len:
        return None

    msg_len = struct.unpack(">I", raw_len)[0]

    data = b""
    while len(data) < msg_len:
        packet = conn.recv(msg_len - len(data))
        if not packet:
            return None
        data += packet

    return json.loads(data.decode("utf-8"))


# ----------------------------------------------------------
# TCP Server → Batch Inference → Kafka
# ----------------------------------------------------------
def start_server():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((HOST, PORT))
    server.listen(5)

    print(f"[INFO] Vision TCP Receiver running on {HOST}:{PORT}")

    while True:
        conn, addr = server.accept()
        print(f"\n[INFO] Sender connected: {addr}")

        while True:
            payload = recv_payload(conn)
            if payload is None:
                print("[WARN] Sender disconnected")
                break

            # -------------------------------
            # Payload is a LIST of frames
            # -------------------------------
            frames_list = payload  # <-- FIXED here

            if not isinstance(frames_list, list):
                print("[ERROR] Received payload is not a list:", payload)
                conn.sendall(b"BAD_FORMAT")
                continue

            if not frames_list:
                print("[WARN] Empty batch received")
                conn.sendall(b"EMPTY")
                continue

            print(f"[INFO] Received batch of {len(frames_list)} frames")

            # -------------------------------
            # Convert all frames
            # -------------------------------
            model_frames = []
            for f in frames_list:
                img = decode_image(f["frameData"])
                model_frames.append({
                    "camera_id": f["cameraId"],
                    "frame": img,
                    "frame_id": f["frameId"],
                    "timestamp": f["timestamp"],
                })

            # -------------------------------
            # 1️⃣ PPE Inference
            # -------------------------------
            print("[INFO] Running PPE model...")
            ppe_out = ppe.batch_infer(model_frames)
            ppe.forward_to_producer(pobj, ppe_out)
            
            print("sent to producer")

            # -------------------------------
            # 2️⃣ Person Detection Inference
            # -------------------------------
            # print("[INFO] Running Person Detection model...")
            # person_out = person_detector.batch_infer(model_frames)
            # person_detector.forward_to_producer(pobj, person_out)

            conn.sendall(b"OK")


        conn.close()


# ----------------------------------------------------------
# Run server
# ----------------------------------------------------------
if __name__ == "__main__":
    start_server()
