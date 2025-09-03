import logging
import re
import sys
from pathlib import Path

import boto3

from FaaSr_py.config.debug_config import global_config

logger = logging.getLogger(__name__)


def faasr_get_file(
    faasr_payload,
    local_file,
    remote_file,
    server_name="",
    local_folder=".",
    remote_folder=".",
):
    """
    Download file from S3 or local file system
    """
    # Clean folder and file paths
    remote_folder = re.sub(r"/+", "/", str(remote_folder).rstrip("/"))
    remote_file = re.sub(r"/+", "/", str(remote_file).rstrip("/"))
    local_folder = re.sub(r"/+", "/", str(local_folder).rstrip("/"))
    local_file = re.sub(r"/+", "/", str(local_file).rstrip("/"))

    get_file_local = Path(local_folder) / local_file
    get_file_remote = Path(remote_folder) / remote_file

    if global_config.USE_LOCAL_FILE_SYSTEM:
        remote_path = Path(global_config.LOCAL_FILE_SYSTEM_DIR) / get_file_remote
        get_file_local.parent.mkdir(parents=True, exist_ok=True)
        with open(remote_path, "r") as rf, open(get_file_local, "w") as lf:
            lf.write(rf.read())
    else:
        if not server_name:
            if "DefaultDataStore" in faasr_payload:
                server_name = faasr_payload["DefaultDataStore"]
            else:
                logger.error("No default data store")
                raise RuntimeError("No default data store")
        if server_name not in faasr_payload["DataStores"]:
            logger.error(f"Invalid data server name: {server_name}")
            sys.exit(1)

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
            s3_client.download_file(
                Bucket=target_s3["Bucket"],
                Key=str(get_file_remote),
                Filename=str(get_file_local),
            )
        except s3_client.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                logger.error(
                    f"S3 object not found: s3://{target_s3['Bucket']}/{get_file_remote}"
                )
            else:
                logger.error(f"Error downloading file from S3: {e}")
            sys.exit(1)

        logger.debug(f"File successfully downloaded to {get_file_local}")
