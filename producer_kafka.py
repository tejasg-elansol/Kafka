import cv2
import time
import json
import base64
from datetime import datetime
from kafka import KafkaProducer
from ultralytics import YOLO
from custom_partitioner import custom_safety_partitioner

class SafetyClusterProducer:
    def __init__(self, rtsp_url="rtsp://admin:Elansol%402020@192.168.0.93:554/Streaming/Channels/101"):
        """
        Initialize Safety Cluster Producer with RTSP stream and custom partitioner
        
        Args:
            rtsp_url: RTSP stream URL (change to your camera URL)
        """
        
        # Kafka Producer with custom partitioner
        # self.producer = KafkaProducer(
        #     bootstrap_servers=['localhost:9092'],
        #     value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        #     partitioner=custom_safety_partitioner
        # )
        self.producer = KafkaProducer(
        bootstrap_servers=['192.168.0.56:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        partitioner=custom_safety_partitioner
    )

        # Topic mapping for 6 safety use cases
        self.topics = {
            "PPE": "safety.ppe",
            "FIRE": "safety.fire",
            "FALL": "safety.fall",
            "VEHICLE": "safety.vehicle",
            "EXIT": "safety.exit",
            "CROWD": "safety.crowd"
        }
        
        # Load PPE Detection Model
        print("Loading PPE Detection Model...")
        self.ppe_model = YOLO("/home/scout/scout_devlopment_vision/model/ppe.pt")
        print(f" Loaded PPE model with classes: {self.ppe_model.names}")
        
        # RTSP Stream Setup
        self.rtsp_url = rtsp_url
        self.cap = None
        self.camera_id = "CAM_001"
        self.message_count = 0
        self.partition_tracker = {}
        
    def connect_rtsp(self):
        """Connect to RTSP stream"""
        print(f"\n Connecting to RTSP stream: {self.rtsp_url}")
        self.cap = cv2.VideoCapture(self.rtsp_url)
        
        # Set stream properties for better performance
        self.cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
        self.cap.set(cv2.CAP_PROP_FPS, 30)
        
        if not self.cap.isOpened():
            print(" Could not connect to RTSP stream.")
            return False
        
        print(" Successfully connected to RTSP stream")
        return True
    
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


    
    def send_alert(self, use_case, data, camera_id, frame=None):
        """
        Send alert to appropriate topic with custom partitioner
        
        Args:
            use_case: Type of detection (PPE, FIRE, etc)
            data: Detection data
            camera_id: Source camera ID (used as partition key)
        """
        if use_case not in self.topics:
            print(f" Unknown use case: {use_case}")
            return
        
        topic = self.topics[use_case]
        
        frame_b64 = self.encode_frame(frame) if frame is not None else None
        
        print("Frame b64 length:", len(frame_b64) if frame_b64 else "None")
        
        # Create message
        message = {
            "use_case": use_case,
            "camera_id": camera_id,
            "timestamp": datetime.now().isoformat(),
            "data": data,
            "frame_320x320": frame_b64
        }
        
        # Send with camera_id as partition key
        try:
            future = self.producer.send(
                topic,
                key=camera_id.encode('utf-8'),
                value=message
            )
            
            record_metadata = future.get(timeout=10)
            self.message_count += 1
            
            # Track partition
            partition_key = f"{topic}:{record_metadata.partition}"
            if partition_key not in self.partition_tracker:
                self.partition_tracker[partition_key] = 0
            self.partition_tracker[partition_key] += 1
            
            print(f"\n [{self.message_count}] {use_case} Alert Sent")
            print(f"   └─ Topic: {topic} | Partition: {record_metadata.partition} | Offset: {record_metadata.offset}")
            
        except Exception as e:
            print(f"Error sending {use_case}: {e}")
    
    def detect_ppe(self, frame, confidence_threshold=0.5):
        """
        Perform PPE detection using YOLO model
        
        Args:
            frame: Video frame
            confidence_threshold: Minimum confidence for detection
        """
        try:
            results = self.ppe_model(frame, verbose=False)
            
            for result in results:
                boxes = result.boxes
                
                if len(boxes) == 0:
                    return  # No detections
                
                for box in boxes:
                    cls_id = int(box.cls[0])
                    confidence = float(box.conf[0])
                    label = self.ppe_model.names[cls_id]
                    
                    if confidence >= confidence_threshold:
                        # Prepare detection data
                        data = {
                            "object": label,
                            "confidence": round(confidence, 3),
                            "bbox": {
                                "x1": float(box.xyxy[0][0]),
                                "y1": float(box.xyxy[0][1]),
                                "x2": float(box.xyxy[0][2]),
                                "y2": float(box.xyxy[0][3])
                            }
                        }
                        
                        # Send PPE detection alert
                        self.send_alert("PPE", data, self.camera_id, frame)
                        
                        # Draw bounding box on frame (optional visualization)
                        self.draw_bbox(frame, box, label, confidence)
        
        except Exception as e:
            print(f"PPE Detection Error: {e}")
    
    def draw_bbox(self, frame, box, label, confidence):
        """Draw bounding box on frame"""
        try:
            x1, y1, x2, y2 = box.xyxy[0]
            x1, y1, x2, y2 = int(x1), int(y1), int(x2), int(y2)
            
            # Draw rectangle
            cv2.rectangle(frame, (x1, y1), (x2, y2), (0, 255, 0), 2)
            
            # Put label
            label_text = f"{label} {confidence:.2f}"
            cv2.putText(frame, label_text, (x1, y1 - 10),
                       cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 0), 2)
        
        except Exception as e:
            print(f"Drawing error: {e}")
    
    def start_streaming(self):
        """Start processing RTSP stream and sending alerts"""
        if not self.connect_rtsp():
            return
        
        print("\n" + "="*80)
        print("SAFETY CLUSTER - PPE DETECTION STARTED")
        print("="*80)
        print(f"Camera ID: {self.camera_id}")
        print(f"Topics: {', '.join(self.topics.values())}")
        print(f"Partitioner: Custom Safety Partitioner")
        print("="*80 + "\n")
        
        frame_count = 0
        
        try:
            while True:
                ret, frame = self.cap.read()
                
                if not ret:
                    print("Failed to read frame from RTSP stream.")
                    time.sleep(5)
                    print("Attempting to reconnect...")
                    self.connect_rtsp()
                    continue
                
                frame_count += 1
                
                # Resize frame for faster processing
                frame = cv2.resize(frame, (640, 480))
                
                # Perform PPE detection
                self.detect_ppe(frame, confidence_threshold=0.5)
                
                # Display frame with detections (optional)
                cv2.imshow("PPE Detection - Safety Cluster", frame)
                if cv2.waitKey(1) & 0xFF == ord('q'):
                    break
                
                # Process every frame or skip for performance
                time.sleep(0.1)
        
        except KeyboardInterrupt:
            print("\n\n Stream stopped by user.")
        
        except Exception as e:
            print(f"Stream Error: {e}")
        
        finally:
            self.cleanup()
    
    def print_summary(self):
        """Print summary of messages sent and partition distribution"""
        print("\n" + "="*80)
        print("SAFETY CLUSTER - PRODUCTION SUMMARY")
        print("="*80)
        print(f"Total messages sent: {self.message_count}")
        print(f"\nPartition Distribution:")
        for partition_key, count in sorted(self.partition_tracker.items()):
            topic, partition = partition_key.split(":")
            print(f"   {topic:20} | Partition: {partition} | Messages: {count}")
        print("="*80)
    
    def cleanup(self):
        """Cleanup resources"""
        self.print_summary()
        self.producer.flush()
        self.producer.close()
        if self.cap:
            self.cap.release()
        cv2.destroyAllWindows()
        print("Kafka producer closed and resources released.")

if __name__ == "__main__":
    # Change RTSP URL to your camera
    # Examples:
    # - Hikvision: "rtsp://admin:admin@192.168.1.100:554/stream"
    # - Uniview: "rtsp://admin:admin@192.168.1.101:554/stream"
    # - Local test: 0 (for webcam)
    
    safety_producer = SafetyClusterProducer(
        rtsp_url="rtsp://admin:Elansol%402020@192.168.0.93:554/Streaming/Channels/101"
    )
    
    safety_producer.start_streaming()