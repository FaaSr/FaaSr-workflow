import logging

from FaaSr_py.config.s3_log_sender import S3LogSender

logger = logging.getLogger(__name__)


class S3LogHandler(logging.Handler):
    """
    Handler for Python logger to send logs to S3
    """

    def __init__(self, faasr_payload, level, start_time):
        """
        Arguments:
            faasr_payload: FaaSr payload instance
            level: minimum logging threshold (DEBUG, INFO, WARNING, ERROR, EXCEPTION)
            start_time: timestamp from start of FaaSr action
        """
        # Initialize the S3LogSender with the provided faasr_payload
        self._sender = S3LogSender(timestamp=start_time, faasr_payload=faasr_payload)
        super().__init__(level=level)

    def emit(self, record):
        try:
            # get timestamp since start of func
            record.timestamp = self._sender.get_curr_timestamp()
            if self.formatter is None:
                formatter = logging.Formatter(
                    "[%(timestamp)s] [%(levelname)s] [%(filename)s] %(message)s"
                )
                self.setFormatter(formatter)

            # format log and send it
            msg = self.format(record)
            self._sender.log(msg)
        except Exception as e:
            self._sender.flush_log()
            raise RuntimeError("failed to upload s3 log") from e

        # flush log if it is an error
        if record.levelno >= logging.ERROR:
            self._sender.flush_log()
