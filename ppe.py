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

try:
    import torch
except:
    torch = None


# ============================================================
# PPE CLASS
# ============================================================
class PPE:
    """
    PPE class manages:
    - YOLO best.pt model loading
    - Batch inference
    - Multi-detection → multi-Kafka messages
    - JSON formatting for each detection
    """

    def __init__(self, model_path: str, camera_id: str, use_case: str = "PPE", device: Optional[str] = None):
        self.model_path = model_path
        self.camera_id = camera_id
        self.use_case = use_case

        self.device = device or ("cuda" if torch and torch.cuda.is_available() else "cpu")

        self.model = None
        self._loaded = False
        self._lock = threading.RLock()
        self._ref_count = 0

    # ============================================================
    # LOAD MODEL
    # ============================================================
    def load_model(self):
        with self._lock:
            if self._loaded:
                return

            # Load Ultralytics YOLO best.pt
            from ultralytics import YOLO
            self.model = YOLO(self.model_path)

            self._loaded = True
            print(f"[PPE] Model loaded on {self.device}")

    # ============================================================
    # RELEASE MODEL
    # ============================================================
    def release_model(self):
        with self._lock:
            if not self._loaded:
                return

            while self._ref_count > 0:
                time.sleep(0.01)

            self.model = None
            self._loaded = False
            print("[PPE] Model released")

    # ============================================================
    # INTERNAL USAGE LOCK
    # ============================================================
    def _acquire(self):
        with self._lock:
            if not self._loaded:
                raise RuntimeError("Model not loaded. Call load_model() first.")
            self._ref_count += 1

    def _release(self):
        with self._lock:
            self._ref_count = max(0, self._ref_count - 1)

    # ============================================================
    # BATCH INFERENCE
    # frames: [{"image": np.ndarray, "frame_id": int, "timestamp": str}, ...]
    # ============================================================
    def batch_infer(self, frames: List[Dict[str, Any]], batch_size: int = 8) -> List[Dict[str, Any]]:
        if not self._loaded:
            raise RuntimeError("Call load_model() before inference.")

        results = []
        self._acquire()

        try:
            for i in range(0, len(frames), batch_size):
                batch = frames[i:i + batch_size]

                imgs = [f["image"] for f in batch]

                # Run YOLO inference directly on list of numpy images
                yolo_out = self.model(imgs, verbose=False)

                # Parse each frame result
                for frame_info, det in zip(batch, yolo_out):
                    detections = []

                    for box in det.boxes:
                        xyxy = box.xyxy[0].tolist()
                        conf = float(box.conf[0])
                        cls_id = int(box.cls[0])

                        detections.append({
                            "object": det.names[cls_id],   # map class ID → class name
                            "confidence": conf,
                            "bbox": {
                                "x1": xyxy[0],
                                "y1": xyxy[1],
                                "x2": xyxy[2],
                                "y2": xyxy[3],
                            }
                        })

                    results.append({
                        "frame_id": frame_info["frame_id"],
                        "timestamp": frame_info["timestamp"],
                        "detections": detections
                    })

            return results

        finally:
            self._release()

    # PROCESS RAW → MULTIPLE KAFKA MESSAGES

    def process_raw(self, raw_outputs: List[Dict[str, Any]]) -> List[bytes]:
        kafka_messages = []

        for frame in raw_outputs:
            frame_id = frame["frame_id"]
            frame_ts = frame["timestamp"]
            detections = frame.get("detections", [])

            for det in detections:
                payload = {
                    "use_case": self.use_case,
                    "camera_id": self.camera_id,
                    "timestamp": frame_ts,
                    "frame_id": frame_id,
                    "data": {
                        "object": det["object"],
                        "confidence": det["confidence"],
                        "bbox": det["bbox"]
                    }
                }

                kafka_messages.append(json.dumps(payload).encode("utf-8"))

        return kafka_messages

    # ============================================================
    # SEND TO KAFKA
    # ============================================================
    def send_to_kafka(self, producer, topic: str, payloads: List[bytes]):
        for payload in payloads:
            producer.send(
                topic,
                key=self.camera_id.encode("utf-8"),   # Key = camera_id
                value=payload
            )

