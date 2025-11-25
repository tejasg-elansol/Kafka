# ppe_logic.py

from ultralytics import YOLO
from datetime import datetime

class PPELogic:
    """
    This class handles:
    - Loading the PPE YOLO model
    - Running detection on each frame
    - Preparing the message in EXACT SAME FORMAT you currently use
    - Triggering producer.send() to send the message to Kafka
    """

    def __init__(self, producer, camera_id):
        self.producer = producer
        self.camera_id = camera_id
        self.model = YOLO("/home/scout/scout_devlopment_vision/model/ppe.pt")

        print("[PPE] Model loaded")

    def process_frame(self, frame, confidence_threshold=0.5):
        """
        Runs PPE detection on a single frame and sends the message.

        This is the replacement for detect_ppe() inside your producer.
        """

        results = self.model(frame, verbose=False)

        for result in results:
            boxes = result.boxes

            if len(boxes) == 0:
                return  # No person detected, skip sending message

            for box in boxes:

                cls_id = int(box.cls[0])
                confidence = float(box.conf[0])

                if confidence < confidence_threshold:
                    continue

                label = self.model.names[cls_id]

                x1, y1, x2, y2 = box.xyxy[0].tolist()

                # 🔥 Build EXACT SAME MESSAGE FORMAT you already use
                message = {
                    "use_case": "PPE",
                    "camera_id": self.camera_id,
                    "timestamp": datetime.now().isoformat(),
                    "data": {
                        "object": label,
                        "confidence": round(confidence, 3),
                        "bbox": {
                            "x1": x1,
                            "y1": y1,
                            "x2": x2,
                            "y2": y2
                        }
                    },
                    "frame_320x320": self.producer.encode_frame(frame)
                }

                # 🔥 Send final message to Kafka
                self.producer.send(
                    topic_key="PPE",
                    key=self.camera_id,
                    value=message
                )
