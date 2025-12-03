import cv2, queue, threading, time
import logging
import logging.handlers


class VideoCapture:

  def __init__(self, name, log_name, camera_label=None):
        self.camera_label = camera_label if camera_label else name
        self.name = name  # <- store source for reconnection

        # Initialize logger
        self.logger = logging.getLogger(self.camera_label)
        self.logger.setLevel(logging.DEBUG)

        log_handler = logging.handlers.RotatingFileHandler(
            log_name, maxBytes=2048 * 2048, backupCount=5
        )
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        log_handler.setFormatter(formatter)
        self.logger.addHandler(log_handler)

        # Initialize capture
        if name == '0':
            self.cap = cv2.VideoCapture(int(name))
        else:
            self.cap = cv2.VideoCapture(name)

        self.q = queue.Queue()

        if "rtsp" in name:
            t = threading.Thread(target=self._reader)
            t.daemon = True
            t.start()

  def _open_capture(self):
        """(helper) Open self.cap based on self.name without changing original logic."""
        try:
            # close old handle if any
            try:
                if hasattr(self, "cap") and self.cap is not None:
                    self.cap.release()
            except Exception:
                pass

            if self.name == '0':
                self.cap = cv2.VideoCapture(int(self.name))
            else:
                self.cap = cv2.VideoCapture(self.name)
        except Exception as e:
            self.logger.error(f"Error reopening capture: {e}")

  def _attempt_reconnect(self):
        """
        Try to reconnect with backoff. Returns True if reconnected, False otherwise.
        Keeps trying but yields time so caller loop can continue.
        """
        self.logger.warning("Stream read failed. Starting reconnection attempts...")
        backoff = [0.5, 1, 2, 3, 5]  # seconds, then stick to 5s
        idx = 0
        while True:
            self._open_capture()
            # tiny wait to allow RTSP/TCP handshake
            time.sleep(0.2)
            if hasattr(self, "cap") and self.cap is not None and self.cap.isOpened():
                # sanity read to confirm frames actually flow
                ok, _ = self.cap.read()
                if ok:
                    self.logger.info("Reconnected to stream successfully.")
                    return True
            delay = backoff[idx] if idx < len(backoff) else backoff[-1]
            self.logger.warning(f"Reconnect failed. Retrying in {delay:.1f}s ...")
            time.sleep(delay)
            idx += 1

  def _reader(self):
    """
    Continuously reads frames from the video capture (for RTSP streams).
    This runs on a separate thread to avoid blocking the main thread.
    """
    while True:
      start = (time.time()) * 1000  # Get the current time in milliseconds
      ret, frame = self.cap.read()  # Read a frame from the video capture object

      if not ret:  # If no frame is captured (ret = False)
        self.q.put([None, start])  # Put None in the queue to signal the error
        self.logger.critical("ret false")  # Log a critical error message
        # ---- reconnection logic (added) ----
        self._attempt_reconnect()
        continue  # continue reading after reconnect

      if not self.q.empty():  # Check if the queue already has an item
        try:
          self.q.get_nowait()  # Remove an old frame from the queue if it exists
        except queue.Empty as QE:
          self.logger.critical("Error in _reader Function :" + str(QE))  # Log an error if queue is empty

      self.q.put([frame, start])  # Add the captured frame and timestamp to the queue

  def vid_reader(self):
    """
    Read a single frame from the video capture (non-threaded).
    """
    start = (time.time()) * 1000  # Get the current time in milliseconds
    ret, frame = self.cap.read()  # Capture a frame
    if not ret:  # If reading the frame failed
      # ---- reconnection logic (added) ----
      self._attempt_reconnect()
      # try one more immediate read after reconnect
      start = (time.time()) * 1000
      ret, frame = self.cap.read()
      if not ret:
        return  # Return None (no frame captured)
    return [frame, start]  # Return the captured frame along with the timestamp

  def read(self):
    """
    Retrieves the next frame from the queue.
    """
    try:
      return self.q.get()  # Get the next frame from the queue
    except Exception as E:
      self.logger.critical("Error in read Function :" + str(E))  # Log any exceptions that occur while reading
