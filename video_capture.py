import cv2
import time


# -------- CAMERA URLS --------
CAMERAS = {
    "cam1": "rtsp://admin:Elansol%402020@192.168.0.92:554/Streaming/Channels/101",
    "cam2": "rtsp://admin:Elansol%402020@192.168.0.93:554/Streaming/Channels/101",
    "cam3": "rtsp://admin:Elansol%402020@192.168.0.94:554/Streaming/Channels/101"
}

BATCH_SIZE = 8


def capture_frames():
    caps = {}

    # open all 3 cameras
    for cam_id, url in CAMERAS.items():
        print(f"Opening {cam_id}...")
        cap = cv2.VideoCapture(url)
        if not cap.isOpened():
            print(f"ERROR: {cam_id} could not be opened!")
        caps[cam_id] = cap

    # storage for batches
    batches = {cam_id: [] for cam_id in CAMERAS}

    print("\nStarting capture...\n")

    while True:

        for cam_id, cap in caps.items():

            if len(batches[cam_id]) >= BATCH_SIZE:
                continue  # already full

            ret, frame = cap.read()
            if not ret:
                print(f"[{cam_id}] frame not received!")
                continue

            batches[cam_id].append(frame)
            print(f"[{cam_id}] Collected {len(batches[cam_id])}/{BATCH_SIZE}")

        # check if all cameras finished
        if all(len(batches[c]) == BATCH_SIZE for c in CAMERAS):
            print("\nAll 3 cameras collected 8 frames each!")
            break

        time.sleep(0.02)

    # release cameras
    for cap in caps.values():
        cap.release()

    return batches


if __name__ == "__main__":
    batches = capture_frames()

    # Example: show batch sizes
    for cam_id, batch in batches.items():
        print(f"{cam_id}: {len(batch)} frames captured")
