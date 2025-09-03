import logging
import re
import sys
from pathlib import Path

import boto3

from FaaSr_py.config.debug_config import global_config

logger = logging.getLogger(__name__)


def faasr_put_file(
    faasr_payload,
    local_file,
    remote_file,
    server_name="",
    local_folder=".",
    remote_folder=".",
):
    """
    Uploads a file to S3 bucket

    Arguments:
        faasr_payload: FaaSr payload dict
        local_file: str -- name of local file to upload
        remote_file: str -- name of file to upload to S3
        server_name: str -- name of S3 data store to put file in
        local_folder: str -- local folder to upload file from
        remote_folder: str -- folder in S3 to put file in
    """

    # Remove "/" in the folder & file name to avoid situations:
    # 1: duplicated "/" ("/remote/folder/", "/file_name")
    # 2: multiple "/" by user mistakes ("//remote/folder//", "file_name")
    # 3: file_name ended with "/" ("/remote/folder", "file_name/")
    remote_folder = re.sub(r"/+", "/", str(remote_folder).rstrip("/"))
    remote_file = re.sub(r"/+", "/", str(remote_file).rstrip("/"))

    # Path for remote file
    remote_path = Path(remote_folder) / remote_file

    # If the local file exists in the current working directory,
    # then set local_path to the name of the file
    # Otherwise, construct valid path to the file
    local_file_path = Path(local_file)

    # Check if local_folder is "." and local_file contains path information
    if local_folder == "." and str(local_file_path) != local_file_path.name:
        # local_file has directory components
        local_folder = str(local_file_path.parent)
        local_path = Path(local_file)
    else:
        # remove trailing '/' and replace instances of multiple '/' in a row with '/'
        local_folder = re.sub(r"/+", "/", local_folder.rstrip("/"))
        local_file = re.sub(r"/+", "/", local_file.rstrip("/"))
        local_path = Path(local_folder) / local_file

    if not local_path.exists():
        raise FileNotFoundError(f"Local file not found: {local_path}")

    if global_config.USE_LOCAL_FILE_SYSTEM:
        path_to_put = Path(global_config.LOCAL_FILE_SYSTEM_DIR) / remote_path
        path_to_put.parent.mkdir(parents=True, exist_ok=True)
        with open(local_path, "r") as rf:
            with open(path_to_put, "w") as wf:
                wf.write(rf.read())
    else:
        # Get the server name from payload if it is not provided
        if server_name == "":
            server_name = faasr_payload["DefaultDataStore"]

        # Ensure that the server name is valid
        if server_name not in faasr_payload["DataStores"]:
            logger.error(f"Invalid data server name: {server_name}")
            sys.exit(1)

        # Get the S3 server to put the file in
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

        try:
            with open(local_path, "rb") as put_data:
                s3_client.put_object(
                    Bucket=target_s3["Bucket"], Body=put_data, Key=str(remote_path)
                )
        except s3_client.exceptions.ClientError as e:
            logger.error(f"Error putting file in S3: {e}")
            sys.exit(1)

        logger.debug(f"File {local_file} successfully uploaded to {remote_path}")
