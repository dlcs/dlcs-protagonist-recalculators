from urllib.parse import urlparse

import psycopg2
from app import aws_factory
from logzero import logger


def connect_to_postgres(connection_info, connection_timeout: str):
    try:
        logger.debug("connecting to postgres")
        conn = psycopg2.connect(**connection_info, connect_timeout=connection_timeout)
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        raise e


def get_connection_config(connection_string: str, aws_connection_string_location: str):
    connection_string = __get_connection_string(connection_string=connection_string, aws_connection_string_location=aws_connection_string_location)

    result = urlparse(connection_string)
    username = result.username
    password = result.password
    database = result.path[1:]
    hostname = result.hostname
    port = result.port

    return {
        "database": database,
        "user": username,
        "password": password,
        "host": hostname,
        "port": port
    }


def __get_connection_string(connection_string: str, aws_connection_string_location: str):

    if connection_string is not None:
        return connection_string
    else:
        logger.debug("retrieving connection string from AWS")
        try:
            ssm = aws_factory.get_aws_client("ssm")
            parameter = ssm.get_parameter(Name=aws_connection_string_location, WithDecryption=True)
            return parameter["Parameter"]["Value"]
        except Exception as e:
            logger.error(f"Error retrieving ssm parameter: {e}")
            raise e