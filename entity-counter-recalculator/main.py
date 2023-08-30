import psycopg2
from psycopg2 import extras

from logzero import logger
from urllib.parse import urlparse
from app.settings import (CONNECTION_STRING, DRY_RUN, ENABLE_CLOUDWATCH_INTEGRATION,
                          CLOUDWATCH_CUSTOMER_DELETE_METRIC_NAME, CLOUDWATCH_SPACE_DELETE_METRIC_NAME,
                          CLOUDWATCH_CUSTOMER_DIFFERENCE_METRIC_NAME, CLOUDWATCH_SPACE_DIFFERENCE_METRIC_NAME,
                          CONNECTION_TIMEOUT, APP_VERSION, AWS_CONNECTION_STRING_LOCATION)
import app.aws_factory


def begin_cleanup():
    connection_info = __get_connection_config()
    conn = __connect_to_postgres(connection_info)
    records = __run_sql(conn)

    if ENABLE_CLOUDWATCH_INTEGRATION:
        logger.info("setting cloudwatch metrics")
        cloudwatch = app.aws_factory.get_aws_client("cloudwatch")
        __set_cloudwatch_metrics(records, cloudwatch, connection_info)

    conn.close()

    return records


def __set_cloudwatch_metrics(records, cloudwatch, connection_info):
    customer_delta = 0
    customer_deletes_needed = 0
    metric_data = []
    dimensions = [
                {
                    'Name': 'TABLE_NAME',
                    'Value': connection_info["database"]
                },
                {
                    'Name': 'APP_VERSION',
                    'Value': APP_VERSION
                }]

    for key in records["customerImages"]:
        logger.info(f"customer images delta found - {key}")
        if key["delta"] is not None:
            customer_delta += abs(key["delta"])
        else:
            customer_deletes_needed += 1

    metric_data.extend([
        {
            'MetricName': CLOUDWATCH_CUSTOMER_DIFFERENCE_METRIC_NAME,
            'Dimensions': dimensions,
            'Unit': 'None',
            'Value': customer_delta
        },
        {
            'MetricName': CLOUDWATCH_CUSTOMER_DELETE_METRIC_NAME,
            'Dimensions': dimensions,
            'Unit': 'None',
            'Value': customer_deletes_needed
        }
    ])

    space_delta = 0
    space_deletes_needed = 0

    for key in records["spaceImages"]:
        logger.info(f"space images delta found - {key}")
        if key["delta"] is not None:
            space_delta += abs(key["delta"])
        else:
            space_deletes_needed += 1

    metric_data.extend([
        {
            'MetricName': CLOUDWATCH_SPACE_DIFFERENCE_METRIC_NAME,
            'Dimensions': dimensions,
            'Unit': 'None',
            'Value': space_delta
        },
        {
            'MetricName': CLOUDWATCH_SPACE_DELETE_METRIC_NAME,
            'Dimensions': dimensions,
            'Unit': 'None',
            'Value': space_deletes_needed
        }
    ])

    try:
        logger.debug(f"updating cloudwatch metrics - {metric_data}")
        cloudwatch.put_metric_data(MetricData=metric_data,
                                   Namespace='Entity Counter Recalculator')
    except Exception as e:
        logger.error(f"Error posting to cloudwatch: {e}")
        raise e


def __run_sql(conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # update customer images
    cur.execute("""
        WITH cte AS (
        SELECT 'customer-images', x.count::bigint, x.customer
            FROM (SELECT count(*) as count, "Customer" as customer
              FROM "Images"
              GROUP BY customer) as x), ins as (
        INSERT INTO "EntityCounters" as y
        SELECT 'customer-images', cte.customer, cte.count::bigint, 0
        FROM cte
        ON CONFLICT ("Type", "Scope", "Customer")
        DO UPDATE SET ("Type", "Scope", "Next", "Customer") = ROW (excluded.*)
        RETURNING *)
        -- selects with `with` execute on snapshots, so changes from the upsert aren't seen here.
        -- See https://www.postgresql.org/docs/11/queries-with.html
        SELECT "Scope" as customer, count, "Next", count - "Next" as delta
        FROM "EntityCounters" as y
          LEFT OUTER JOIN cte ON cte.customer = "Scope"::int
            where y."Type" = 'customer-images' and coalesce(count - "Next", -1) != 0
             order by "Scope"::int;
    """)

    customer_image_changes = cur.fetchall()

    cur.execute("""
        WITH cte AS (
        SELECT 'space-images', x.space, x.count::bigint, x.customer
            FROM (SELECT count(*) as count, "Space" as space, "Customer" as customer
              FROM "Images"
              GROUP BY customer, space
              ORDER BY customer, space) as x), ins as
                  (
        INSERT INTO "EntityCounters" as y
        SELECT 'space-images', cte.space, cte.count::bigint, cte.customer
        FROM cte
        ON CONFLICT ("Type", "Scope", "Customer")
        DO UPDATE SET ("Type", "Scope", "Next", "Customer") = ROW (excluded.*)
        RETURNING *)
        -- selects with `with` execute on snapshots, so changes from the upsert aren't seen here.
        -- See https://www.postgresql.org/docs/11/queries-with.html
        SELECT "Customer", "space","Scope", count, "Next", count - "Next" as delta
        FROM "EntityCounters" as y
          LEFT OUTER JOIN cte ON cte.customer = y."Customer" and cte.space::varchar = y."Scope"
            where y."Type" = 'space-images' and coalesce(count - "Next", -1) != 0
             order by "Customer", "Scope";
        """)

    space_image_changes = cur.fetchall()

    records = {
        "customerImages": customer_image_changes,
        "spaceImages": space_image_changes}

    logger.info(records)

    if DRY_RUN:
        logger.info(f"DRY RUN ENABLED.  Changes have not been committed to the database")
    else:
        conn.commit()

    cur.close()

    return records


def __connect_to_postgres(connection_info):
    try:
        logger.debug("connecting to postgres")
        conn = psycopg2.connect(**connection_info, connect_timeout=CONNECTION_TIMEOUT)
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        raise e


def __get_connection_config():
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
            ssm = app.aws_factory.get_aws_client("ssm")
            parameter = ssm.get_parameter(Name=AWS_CONNECTION_STRING_LOCATION, WithDecryption=True)
            return parameter["Parameter"]["Value"]
        except Exception as e:
            logger.error(f"Error retrieving ssm parameter: {e}")
            raise e


if __name__ == "__main__":
    begin_cleanup()


def handler(event, context):
    logger.debug("calling handler...")
    return begin_cleanup()