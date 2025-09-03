import logging
import sys
from pathlib import Path

import boto3

from FaaSr_py.config.debug_config import global_config

logger = logging.getLogger(__name__)


def faasr_get_folder_list(faasr_payload, server_name="", prefix=""):
    """
    Get a list of objects in the S3 bucket

    Arguments:
        faasr_payload: FaaSr payload dict
        server_name: str -- name of S3 data store to get folder list from
        prefix: str -- prefix to filter objects in S3 bucket
    Returns:
        list: List of objects in the S3 bucket with the specified prefix
    """

    if global_config.USE_LOCAL_FILE_SYSTEM:
        logger.info("Getting folder list from local bucket")

        local_bucket = Path(global_config.LOCAL_FILE_SYSTEM_DIR)
        folder_path = local_bucket / prefix

        all_files = [p for p in folder_path.rglob("*") if p.is_file()]

        stripped_files = [str(p.relative_to(local_bucket.parent)) for p in all_files]
        print(stripped_files)
    else:
        # Get server name from payload if one is not providedS
        if server_name == "":
            server_name = faasr_payload["DefaultDataStore"]

        # Ensure the server is a valid data store
        if server_name not in faasr_payload["DataStores"]:
            logger.error("Invalid data server name: {server_name}")
            sys.exit(1)

        # Get the S3 data store to get folder list from
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

        # List objects from S3 bucket
        result = s3_client.list_objects_v2(
            Bucket=target_s3["Bucket"], Prefix=str(prefix)
        )
        if "Contents" in result:
            result = [content["Key"] for content in result["Contents"]]
            result = [obj for obj in result if not obj.endswith("/")]
            return result
        return []
