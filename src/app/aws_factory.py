import boto3


def get_aws_client(resource_type: str, region: str):
    """Get an aws client"""
    return boto3.client(resource_type, region)