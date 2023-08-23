import boto3

from app.settings import LOCALSTACK, REGION, LOCALSTACK_ADDRESS
from logzero import logger


def get_aws_client(resource_type: str):
    """Get an aws resource configured to use LocalStack if env var is set"""
    if LOCALSTACK:
        logger.warn(f"Using localstack for {resource_type} resource")
        return boto3.client(
            resource_type,
            region_name=REGION,
            endpoint_url=LOCALSTACK_ADDRESS,
            aws_access_key_id="foo",
            aws_secret_access_key="bar",  # pragma: allowlist secret
        )
    else:
        return boto3.client(resource_type, REGION)
