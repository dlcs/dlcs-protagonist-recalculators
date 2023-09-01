from urllib.parse import urlparse

import psycopg2
from package.app import aws_factory
from package.app.settings import CONNECTION_STRING, AWS_CONNECTION_STRING_LOCATION, CONNECTION_TIMEOUT
from package.logzero import logger


def connect_to_postgres(connection_info):
    try:
        logger.debug("connecting to postgres")
        conn = psycopg2.connect(**connection_info, connect_timeout=CONNECTION_TIMEOUT)
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        raise e


def get_connection_config():
    connection_string = __get_connection_string()

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


def __get_connection_string():

    if CONNECTION_STRING is not None:
        return CONNECTION_STRING
    else:
        logger.debug("retrieving connection string from AWS")
        try:
            ssm = aws_factory.get_aws_client("ssm")
            parameter = ssm.get_parameter(Name=AWS_CONNECTION_STRING_LOCATION, WithDecryption=True)
            return parameter["Parameter"]["Value"]
        except Exception as e:
            logger.error(f"Error retrieving ssm parameter: {e}")
            raise e