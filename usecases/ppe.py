
"""
PPE Processing Class
--------------------
This class handles:

✔ Model loading (best.pt - Ultralytics YOLO)
✔ Model releasing
✔ Batch inference
✔ Multiple detections → multiple Kafka messages
✔ Each detection = one Kafka JSON message
✔ Uses frame_id and frame_timestamp from upstream RTSP pipeline
✔ Kafka producer only sends raw bytes (no processing in Kafka code)

"""

import json
import threading
import time
from typing import Any, Dict, List, Optional
import numpy as np
from datetime import datetime
from ultralytics import YOLO
import base64 
import cv2

try:
    import torch
except:
    torch = None

# PPE CLASS
class PPE:
    """
    PPE class manages:
    - YOLO best.pt model loading
    - Batch inference
    - Multi-detection → multi-Kafka messages
    - JSON formatting for each detection
    """

    def __init__(self, model_path: str, use_case: str = "PPE",org_id: str = "DEFAULT_ORG", device: Optional[str] = None):
        self.model_path = model_path
        self.use_case = use_case
        self.org_id = org_id
        self.device = device or ("cuda" if torch and torch.cuda.is_available() else "cpu")

        self.model = None
        self._loaded = False
        self._lock = threading.RLock()
        self._ref_count = 0

    # LOAD MODEL
    def load_model(self):
        with self._lock:
            if self._loaded:
                return

            # Load Ultralytics YOLO best.pt
            self.model = YOLO(self.model_path)

            self._loaded = True
            print(f"[{self.use_case}]Model loaded on {self.device}")

    # RELEASE MODEL
    def release_model(self):
        with self._lock:
            if not self._loaded:
                return

            while self._ref_count > 0:
                time.sleep(0.01)

            self.model = None
            self._loaded = False
            print(f"[{self.use_case}] Model released")

    # INTERNAL USAGE LOCK
    
    def _acquire(self):
        with self._lock:
            if not self._loaded:
                raise RuntimeError("Model not loaded. Call load_model() first.")
            self._ref_count += 1

    def _release(self):
        with self._lock:
            self._ref_count = max(0, self._ref_count - 1)

    # BATCH INFERENCE
    # frames: [{"image": np.ndarray, "frame_id": int, "timestamp": str}, ...]
    def batch_infer(self, frames: List[Dict[str, Any]], batch_size: int = 8) -> List[Dict[str, Any]]:
        """
        Run YOLO batch inference.
        Expected frame format:
        {
            "camera_id": ...,
            "frame": numpy_image,
            "frame_id": ...,
            "timestamp": ...
        }
        """
        if not self._loaded:
            raise RuntimeError("Call load_model() before inference.")

        results = []
        self._acquire()

        try:
            for i in range(0, len(frames), batch_size):
                batch = frames[i:i + batch_size]

                # Extract image list from "frame"
                imgs = [f["frame"] for f in batch]

                # Perform YOLO inference on batch
                yolo_out = self.model(imgs, verbose=False)

                # Process each output
                for frame_info, det in zip(batch, yolo_out):
                    detections = []

                    for box in det.boxes:
                        xyxy = box.xyxy[0].tolist()
                        conf = float(box.conf[0])
                        cls_id = int(box.cls[0])

                        detections.append({
                            "object": det.names[cls_id],
                            "confidence": conf,
                            "bbox": {
                                "x1": xyxy[0],
                                "y1": xyxy[1],
                                "x2": xyxy[2],
                                "y2": xyxy[3],
                            }
                        })

                    results.append({
                        "camera_id": frame_info["camera_id"],
                        "frame_id": frame_info["frame_id"],
                        "timestamp": frame_info["timestamp"],
                        "frame":frame_info["frame"],
                        "detections": detections
                    })

            return results

        finally:
            self._release()

    # PROCESS RAW → MULTIPLE KAFKA MESSAGES
    def process_raw(self, raw_outputs: List[Dict[str, Any]]) -> List[bytes]:
        kafka_messages = []

        for frame in raw_outputs:
            camera_id = frame["camera_id"]
            frame_id = frame["frame_id"]
            frame_ts = frame["timestamp"]
            detections = frame.get("detections", [])
            frame_img = frame["frame"]

            # Encode frame only once per frame
            frame_b64 = self.encode_frame(frame_img) if frame_img is not None else None

            # Build ONE message per frame with ALL detections
            payload = {
                "org_id": self.org_id,
                "use_case": self.use_case,
                "camera_id": camera_id,
                "timestamp": frame_ts,
                "frame_id": frame_id,
                "frame": frame_b64,
                "detections": detections  # <-- ALL detections together
            }

            # kafka_messages.append(json.dumps(payload).encode("utf-8"))
            kafka_messages.append(payload)

        return kafka_messages

    def encode_frame(self, frame):
        # Resize to 320x320
        resized = cv2.resize(frame, (320, 320))

        # Fix RTSP camera non-contiguous frame issue
        resized = resized.copy()

        # Encode as JPEG
        success, buffer = cv2.imencode(".jpg", resized)
        if not success:
            print("⚠️ Encoding failed!")
            return None

        return base64.b64encode(buffer).decode('utf-8')
    
    # SEND TO KAFKA
    def forward_to_producer(self, producer, raw_outputs):
        messages = self.process_raw(raw_outputs)

        for msg in messages:
            camera_id = msg["camera_id"]   # key for partitioning

            producer.send(
                use_case=self.use_case,
                key=camera_id,
                message_dict=msg
            )

