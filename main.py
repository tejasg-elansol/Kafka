# main.py
import cv2
from producer_kafka import SafetyProducer
from usecases.ppe_logic import PPELogic

def main():
    camera_id = "CAM_001"
    rtsp_url = "rtsp://admin:Elansol%402020@192.168.0.93:554/Streaming/Channels/101"

    producer = SafetyProducer()
    ppe = PPELogic(producer, camera_id)

    cap = cv2.VideoCapture(rtsp_url)

    if not cap.isOpened():
        print("Failed to connect to RTSP")
        return

    while True:
        ret, frame = cap.read()
        if not ret:
            continue

        frame = cv2.resize(frame, (640, 480))

        # Run PPE logic
        ppe.process_frame(frame)

        cv2.imshow("PPE Stream", frame)
        if cv2.waitKey(1) == ord("q"):
            break

    cap.release()
    producer.close()
    cv2.destroyAllWindows()

if __name__ == "__main__":
    main()
