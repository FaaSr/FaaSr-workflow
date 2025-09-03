import logging
import re
import sys
from pathlib import Path

import boto3

from FaaSr_py.config.debug_config import global_config

logger = logging.getLogger(__name__)


def faasr_delete_file(faasr_payload, remote_file, server_name="", remote_folder=""):
    """
    Deletes a file from S3

    Arguments:
        faasr_payload: FaaSr payload dict
        remote_file: str -- name of file to delete
        server_name: str -- name of S3 data store to delete file from
        remote_folder: str -- folder in S3 to delete file from
    """
    # Remove "/" in the folder & file name to avoid situations:x
    # 1: duplicated "/" ("/remote/folder/", "/file_name")
    # 2: multiple "/" by user mistakes ("//remote/folder//", "file_name")
    # 3: file_name ended with "/" ("/remote/folder", "file_name/")
    remote_folder = re.sub(r"/+", "/", str(remote_folder).rstrip("/"))
    remote_file = re.sub(r"/+", "/", str(remote_file).rstrip("/"))

    # Name of file to delete from S3
    delete_file_path = Path(remote_folder) / remote_file

    if global_config.USE_LOCAL_FILE_SYSTEM:
        full_local_path = Path(global_config.LOCAL_FILE_SYSTEM_DIR) / delete_file_path

        if not full_local_path.exists():
            logger.warning(f"File not found in local bucket: {full_local_path}")
            return

        try:
            full_local_path.unlink()
            logger.info(f"Deleted file from local bucket: {full_local_path}")
        except Exception as e:
            logger.error(f"Failed to delete file locally: {full_local_path} — {e}")
            raise
    else:
        # Get server name from payload if one isn't provided
        if server_name == "":
            server_name = faasr_payload["DefaultDataStore"]

        # Ensure that the server is a valid data store
        if server_name not in faasr_payload["DataStores"]:
            logger.error(f"Invalid data server name: {server_name}")
            sys.exit(1)

        # Get the S3 data store to delete file from
        target_s3 = faasr_payload["DataStores"][server_name]

        if target_s3.get("Endpoint"):
            s3_client = boto3.client(
                "s3",
                aws_access_key_id=target_s3["AccessKey"],
                aws_secret_access_key=target_s3["SecretKey"],
                region_name=target_s3["Region"],
                endpoint_url=target_s3["Endpoint"],
            )
        else:
            s3_client = boto3.client(
                "s3",
                aws_access_key_id=target_s3["AccessKey"],
                aws_secret_access_key=target_s3["SecretKey"],
                region_name=target_s3["Region"],
            )

        # Delete file from S3
        try:
            s3_client.delete_object(
                Bucket=target_s3["Bucket"], Key=str(delete_file_path)
            )
        except s3_client.exceptions.ClientError as e:
            logger.error(f"Error deleting {delete_file_path}: {e}")
            sys.exit(1)

        logger.debug(f"File {remote_file} deleted")
