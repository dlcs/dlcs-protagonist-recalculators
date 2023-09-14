import boto3

from logzero import logger


def get_aws_client(resource_type: str, localstack: bool, region: str, localstack_address: str):
    """Get an aws resource configured to use LocalStack if env var is set"""
    if localstack:
        logger.warn(f"Using localstack for {resource_type} resource")
        return boto3.client(
            resource_type,
            region_name=region,
            endpoint_url=localstack_address,
            aws_access_key_id="foo",
            aws_secret_access_key="bar",  # pragma: allowlist secret
        )
    else:
        return boto3.client(resource_type, region)