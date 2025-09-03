import json
import logging


class JsonFormatter(logging.Formatter):
    """
    Dumps log to json format
    """

    def format(self, record):
        log_record = {
            "level": record.levelname,
            "timestamp": self.formatTime(record, self.datefmt),
            "filename": record.filename,
            "function": record.funcName,
            "lineno": record.lineno,
            "message": record.getMessage(),
            "logger": record.name,
        }
        return json.dumps(log_record)


class FaaSrFilter(logging.Filter):
    """
    Filters out logs from 3rd party packages
    """

    def filter(self, record):
        return record.name.startswith("FaaSr_py")
