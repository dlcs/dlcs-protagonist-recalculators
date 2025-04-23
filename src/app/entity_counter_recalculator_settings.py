import os


def _get_boolean(env_name: str, fallback: str) -> bool:
    return os.environ.get(env_name, fallback).lower() in ("true", "t", "1")


# AWS
REGION = os.environ.get("AWS_REGION", "eu-west-1")
ENABLE_CLOUDWATCH_INTEGRATION = _get_boolean("ENABLE_CLOUDWATCH_INTEGRATION", "True")
CLOUDWATCH_SPACE_DIFFERENCE_METRIC_NAME = os.environ.get("CLOUDWATCH_SPACE_DIFFERENCE_METRIC_NAME", "SpaceTotalDelta")
CLOUDWATCH_SPACE_DELETE_METRIC_NAME = os.environ.get("CLOUDWATCH_SPACE_DELETE_METRIC_NAME", "CustomerDeleteMetrics")
APP_VERSION = os.environ.get('APP_VERSION', "1.0")

# LocalStack
LOCALSTACK = _get_boolean("LOCALSTACK", "False")
LOCALSTACK_ADDRESS = os.environ.get("LOCALSTACK_ADDRESS", "http://localhost:4566")

# Postgres
CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
CONNECTION_TIMEOUT = os.environ.get("CONNECTION_TIMEOUT")
DRY_RUN = _get_boolean("DRY_RUN", "False")
