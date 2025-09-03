import logging
import sys
import uuid
from pathlib import Path

import boto3

from FaaSr_py.config.s3_log_sender import S3LogSender

logger = logging.getLogger(__name__)


def validate_uuid(uuid_value):
    """
    Checks to see if a string is a valid UUID
    """
    if not isinstance(uuid_value, str):
        return False

    try:
        uuid.UUID(uuid_value)
    except ValueError:
        return False
    return True


def get_logging_server(faasr_payload):
    """
    Returns the default logging datastore for the payload as a string

    If LoggingDataStore is None, then returns DefaultDataStore
    """
    if faasr_payload["LoggingDataStore"] is None:
        logging_server = faasr_payload["DefaultDataStore"]
    else:
        logging_server = faasr_payload["LoggingDataStore"]
    return logging_server


def get_default_log_boto3_client(faasr_payload):
    """
    Returns a boto3 client associated with default logging datastore

    Arguments:
        faasr_payload: FaaSr payload dict
    Returns:
        boto3.client: boto3 client for S3 datastore
    """
    # Get the target S3 server
    target_s3 = get_logging_server(faasr_payload)
    s3_log_info = faasr_payload["DataStores"][target_s3]

    if target_s3 not in faasr_payload["DataStores"]:
        err_msg = f"Invalid data server name: {target_s3}"
        logger.error(err_msg)
        sys.exit(1)

    if s3_log_info.get("Endpoint"):
        return boto3.client(
            "s3",
            aws_access_key_id=s3_log_info["AccessKey"],
            aws_secret_access_key=s3_log_info["SecretKey"],
            region_name=s3_log_info["Region"],
            endpoint_url=s3_log_info["Endpoint"],
        )
    else:
        return boto3.client(
            "s3",
            aws_access_key_id=s3_log_info["AccessKey"],
            aws_secret_access_key=s3_log_info["SecretKey"],
            region_name=s3_log_info["Region"],
        )


def flush_s3_log():
    log_sender = S3LogSender.get_log_sender()
    log_sender.flush_log()


def get_invocation_folder(faasr_payload):
    return (
        Path(faasr_payload["FaaSrLog"])
        / Path(faasr_payload["WorkflowName"])
        / Path(faasr_payload["InvocationTimestamp"])
        / faasr_payload["InvocationID"]
    )
