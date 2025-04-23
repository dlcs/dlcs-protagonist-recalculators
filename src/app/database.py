from urllib.parse import urlparse

import psycopg2
from logzero import logger


def connect_to_postgres(connection_info: dict, connection_timeout: str):
    try:
        logger.debug("connecting to postgres")
        conn = psycopg2.connect(**connection_info, connect_timeout=connection_timeout)
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        raise e


def get_connection_config(connection_string: str):

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
