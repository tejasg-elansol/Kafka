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

class PersonDetection:
    """
    Person Detection class manages:
    - YOLO model loading (person best.pt)
    - Batch inference
    - Multiple detections → one Kafka message per frame
    - JSON formatting for each frame
    """

    def __init__(self, model_path: str,
                 use_cases: Optional[List[str]] = None,
                 org_id: str = "DEFAULT_ORG",
                 device: Optional[str] = None):

        self.model_path = model_path
        self.use_cases = use_cases
        self.org_id = org_id
        self.device = device or ("cuda" if torch and torch.cuda.is_available() else "cpu")

        self.model = None
        self._loaded = False
        self._lock = threading.RLock()
        self._ref_count = 0
    
    @property
    def use_case(self):
        if isinstance(self.use_cases, list):
            return self.use_cases[0]
        return self.use_cases

    # LOAD MODEL
    def load_model(self):
        with self._lock:
            if self._loaded:
                return

            self.model = YOLO(self.model_path)
            self._loaded = True

            print(f"[{self.use_case}] Model loaded on {self.device}")
            
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
    def batch_infer(self, frames: List[Dict[str, Any]], batch_size: int = 8) -> List[Dict[str, Any]]:

        if not self._loaded:
            raise RuntimeError("Call load_model() before inference.")

        results = []
        self._acquire()

        try:
            for i in range(0, len(frames), batch_size):
                batch = frames[i : i + batch_size]

                imgs = [f["frame"] for f in batch]

                # YOLO inference
                yolo_out = self.model(imgs, verbose=False)

                for frame_info, det in zip(batch, yolo_out):

                    detections = []

                    # Extract bounding boxes & classes
                    for box in det.boxes:
                        xyxy = box.xyxy[0].tolist()
                        conf = float(box.conf[0])
                        cls_id = int(box.cls[0])
                        label = det.names[cls_id]

                        # Only PERSON class allowed
                        if label.lower() != "person" or conf < 0.4:
                            continue

                        detections.append({
                            "object": "person",
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
                        "frame": frame_info["frame"],
                        "detections": detections
                    })

            return results

        finally:
            self._release()

    # PROCESS RAW → ONE KAFKA MESSAGE PER FRAME
    def process_raw(self, raw_outputs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:

        kafka_messages = []

        for frame in raw_outputs:
            camera_id = frame["camera_id"]
            frame_id = frame["frame_id"]
            frame_ts = frame["timestamp"]
            detections = frame.get("detections", [])
            frame_img = frame["frame"]

            frame_b64 = self.encode_frame(frame_img) if frame_img is not None else None

            payload = {
                "org_id": self.org_id,
                "use_case": self.use_case,
                "camera_id": camera_id,
                "timestamp": frame_ts,
                "frame_id": frame_id,
                "frame": frame_b64,
                "detections": detections
            }

            kafka_messages.append(payload)

        return kafka_messages

    def encode_frame(self, frame):
        resized = cv2.resize(frame, (320, 320))
        resized = resized.copy()

        success, buffer = cv2.imencode(".jpg", resized)
        if not success:
            print(f"[{self.use_case}] Frame encoding failed!")
            return None

        return base64.b64encode(buffer).decode("utf-8")

    # SEND TO KAFKA
    # def forward_to_producer(self, producer, raw_outputs):
    #     messages = self.process_raw(raw_outputs)

    #     for msg in messages:
    #         camera_id = msg["camera_id"]

    #         producer.send(
    #             use_case=self.use_case,
    #             key=camera_id,
    #             message_dict=msg
    #         )
    
    def forward_to_producer(self, producer, raw_outputs, use_cases=None):
        """
        use_cases: list of topics to send message to.
        If None → use default self.use_case
        """
        if use_cases is None:
            use_cases = self.use_cases

        if isinstance(use_cases, str):
            use_cases = [use_cases]

        messages = self.process_raw(raw_outputs)

        for msg in messages:
            camera_id = msg["camera_id"]

            for uc in use_cases:
                # Override the use_case for each topic
                msg["use_case"] = uc

                producer.send(
                    use_case=uc,        # <<< topic name
                    key=camera_id,
                    message_dict=msg
                )

