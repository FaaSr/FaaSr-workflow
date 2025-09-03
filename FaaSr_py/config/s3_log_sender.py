import logging
import sys
from datetime import datetime

logger = logging.getLogger(__name__)


class S3LogSender:
    """
    Sender for S3 dev logs
    """

    _log_sender = None

    def __new__(cls, *args, **kwargs):
        """
        Singleton pattern to ensure only one instance of S3LogSender exists
        """
        if cls._log_sender is None:
            cls._log_sender = super(S3LogSender, cls).__new__(cls)
            cls._log_sender._initialized = False
        return cls._log_sender

    def __init__(self, timestamp, faasr_payload):
        if self._initialized:
            return
        S3LogSender._sender = self
        self._initialized = True
        self._log_buffer = []
        self._start_time = timestamp
        self._faasr_payload = faasr_payload

    @classmethod
    def get_log_sender(cls):
        return cls._log_sender

    @property
    def faasr_payload(self):
        """
        Returns the faasr_payload for the logger
        """
        return self._faasr_payload

    @faasr_payload.setter
    def faasr_payload(self, faasr_payload):
        """
        Sets the faasr_payload for the logger
        """
        self._faasr_payload = faasr_payload

    def log(self, message):
        """
        Adds a message to the log buffer

        Arguments:
            message: str -- message to log
        """
        if not message:
            raise RuntimeError("Cannot log empty message")
        self._log_buffer.append(message)

    def flush_log(self):
        """
        Uploads all messages inside S3LogSender and clears buffer
        """
        if not self._faasr_payload:
            logger.error("S3LogSender payload is not set")
            sys.exit(1)
        if not self._log_buffer:
            return

        # Combine all log messages into a single string and clear buffer
        full_log = "\n".join(self._log_buffer)
        self._log_buffer = []

        from FaaSr_py.s3_api.log import faasr_log

        # Upload the log to S3
        faasr_log(self._faasr_payload, full_log)

    def get_curr_timestamp(self):
        """
        Returns the current timestamp in seconds since the start of the function
        """
        elapsed_time = datetime.now() - self._start_time
        seconds = round(elapsed_time.total_seconds(), 3)
        return seconds
