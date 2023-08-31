import psycopg2
from psycopg2 import extras

from logzero import logger
from urllib.parse import urlparse
from app.settings import (CONNECTION_STRING, DRY_RUN, ENABLE_CLOUDWATCH_INTEGRATION,
                          CLOUDWATCH_CUSTOMER_IMAGE_SIZE_DIFFERENCE_METRIC_NAME,
                          CLOUDWATCH_CUSTOMER_IMAGE_NUMBER_DIFFERENCE_METRIC_NAME,
                          CLOUDWATCH_CUSTOMER_THUMBNAIL_SIZE_DIFFERENCE_METRIC_NAME,
                          CLOUDWATCH_SPACE_IMAGE_SIZE_DIFFERENCE_METRIC_NAME,
                          CLOUDWATCH_SPACE_IMAGE_NUMBER_DIFFERENCE_METRIC_NAME,
                          CLOUDWATCH_SPACE_THUMBNAIL_SIZE_DIFFERENCE_METRIC_NAME,
                          CONNECTION_TIMEOUT, APP_VERSION, AWS_CONNECTION_STRING_LOCATION)
import app.aws_factory


def begin_cleanup():
    connection_info = __get_connection_config()
    conn = __connect_to_postgres(connection_info)
    records = __run_sql(conn)

    if ENABLE_CLOUDWATCH_INTEGRATION:
        logger.info("setting cloudwatch metrics")
        cloudwatch = app.aws_factory.get_aws_client("cloudwatch")
        set_cloudwatch_metrics(records, cloudwatch, connection_info)

    conn.close()

    return records


def set_cloudwatch_metrics(records, cloudwatch, connection_info):
    customer_total_image_size_delta = 0
    customer_total_image_number_delta = 0
    customer_total_thumbnail_size_delta = 0
    metric_data = []
    dimensions = [{
                    'Name': "TABLE_NAME",
                    'Value': connection_info["database"]
                },
                {
                    'Name': "APP_VERSION",
                    'Value': APP_VERSION
                },
                {
                    'Name': "RECALCULATOR_NAME",
                    'Value': "customer_storage_recalculator"
                }]

    for key in records["customerChanges"]:
        logger.info(f"customer delta found - {key}")
        customer_total_image_size_delta += abs(key["totalsizedelta"])
        customer_total_image_number_delta += abs(key["numberofimagesdelta"])
        customer_total_thumbnail_size_delta += abs(key["totalsizeofthumbnailsdelta"])

    metric_data.extend([
        {
            'MetricName': CLOUDWATCH_CUSTOMER_IMAGE_SIZE_DIFFERENCE_METRIC_NAME,
            'Dimensions': dimensions,
            'Unit': 'None',
            'Value': customer_total_image_size_delta
        },
        {
            'MetricName': CLOUDWATCH_CUSTOMER_IMAGE_NUMBER_DIFFERENCE_METRIC_NAME,
            'Dimensions': dimensions,
            'Unit': 'None',
            'Value': customer_total_image_number_delta
        },
        {
            'MetricName': CLOUDWATCH_CUSTOMER_THUMBNAIL_SIZE_DIFFERENCE_METRIC_NAME,
            'Dimensions': dimensions,
            'Unit': 'None',
            'Value': customer_total_thumbnail_size_delta
        }
    ])

    space_total_image_size_delta = 0
    space_total_image_number_delta = 0
    space_total_thumbnail_size_delta = 0

    for key in records["spaceChanges"]:
        logger.info(f"space delta found - {key}")
        space_total_image_size_delta += abs(key["totalsizedelta"])
        space_total_image_number_delta += abs(key["numberofimagesdelta"])
        space_total_thumbnail_size_delta += abs(key["totalsizeofthumbnailsdelta"])

    metric_data.extend([
        {
            'MetricName': CLOUDWATCH_SPACE_IMAGE_SIZE_DIFFERENCE_METRIC_NAME,
            'Dimensions': dimensions,
            'Unit': 'None',
            'Value': space_total_image_size_delta
        },
        {
            'MetricName': CLOUDWATCH_SPACE_IMAGE_NUMBER_DIFFERENCE_METRIC_NAME,
            'Dimensions': dimensions,
            'Unit': 'None',
            'Value': space_total_image_number_delta
        },
        {
            'MetricName': CLOUDWATCH_SPACE_THUMBNAIL_SIZE_DIFFERENCE_METRIC_NAME,
            'Dimensions': dimensions,
            'Unit': 'None',
            'Value': space_total_thumbnail_size_delta
        }
    ])

    try:
        logger.debug(f"updating cloudwatch metrics - {metric_data}")
        cloudwatch.put_metric_data(MetricData=metric_data,
                                   Namespace='Entity Counter Recalculator')
        return metric_data
    except Exception as e:
        logger.error(f"Error posting to cloudwatch: {e}")
        raise e


def __run_sql(conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # update customer images
    cur.execute("""
        with cte AS (SELECT "Customer",
               SUM("Size") AS TotalSizeInImage,
               Count("Id") AS numberOfImagesInImage,
               SUM("ThumbnailSize") AS totalSizeOfThumbnailsInImage
        FROM "ImageStorage" GROUP BY "Customer" ORDER BY "Customer"), ins AS (
        INSERT INTO "CustomerStorage" AS y
               SELECT cte."Customer", 'default', cte.numberOfImagesInImage, cte.TotalSizeInImage, cte.totalSizeOfThumbnailsInImage, current_timestamp, 0
                FROM cte
               ON CONFLICT ("Customer", "Space")
               DO UPDATE SET ("Customer", "StoragePolicy", "NumberOfStoredImages", "TotalSizeOfStoredImages", "TotalSizeOfThumbnails", "LastCalculated", "Space") = ROW(excluded.*)
                   WHERE y."Space" = 0 AND excluded."Customer" IS NOT NULL
               RETURNING *)
        SELECT y."Customer",
               cte."Customer" as customerInImage,
               y."Space",
               "TotalSizeOfStoredImages",
               TotalSizeInImage,
               coalesce("TotalSizeOfStoredImages", 0) - coalesce(TotalSizeInImage, 0) AS TotalSizeDelta,
               "NumberOfStoredImages",
               numberOfImagesInImage,
               coalesce("NumberOfStoredImages", 0) - coalesce(numberOfImagesInImage, 0) AS NumberOfImagesDelta,
               "TotalSizeOfThumbnails",
               totalSizeOfThumbnailsInImage,
               coalesce("TotalSizeOfThumbnails", 0) - coalesce(totalSizeOfThumbnailsInImage, 0) AS TotalSizeOfThumbnailsDelta
        -- selects with `with` execute on snapshots, so changes from the upsert aren't seen here.
        -- See https://www.postgresql.org/docs/11/queries-with.html
        From "CustomerStorage" AS y
        RIGHT OUTER JOIN cte ON y."Customer" = cte."Customer" AND y."Space" = 0
        WHERE   coalesce("TotalSizeOfStoredImages", 0) - coalesce(TotalSizeInImage, 0) != 0 AND cte."Customer" IS NOT NULL
        ORDER BY y."Customer",  cte."Customer";
    """)

    customer_level_changes = cur.fetchall()

    cur.execute("""
        with cte AS (SELECT "Customer", "Space",
               SUM("Size") AS TotalSizeInImage,
               Count("Id") AS numberOfImagesInImage,
               SUM("ThumbnailSize") as totalSizeOfThumbnailsInImage
        FROM "ImageStorage" GROUP BY "Customer", "Space" ORDER BY "Customer", "Space"), ins AS (
        INSERT INTO "CustomerStorage" AS y
               SELECT cte."Customer", 'default', cte.numberOfImagesInImage, cte.TotalSizeInImage, cte.totalSizeOfThumbnailsInImage, current_timestamp, cte."Space"
                FROM cte
               ON CONFLICT ("Customer", "Space")
               DO UPDATE SET ("Customer", "StoragePolicy", "NumberOfStoredImages", "TotalSizeOfStoredImages", "TotalSizeOfThumbnails", "LastCalculated", "Space") = ROW(excluded.*)
               RETURNING *)
        SELECT y."Customer",
               cte."Customer" AS customerInImage,
               y."Space",
               cte."Space" AS spaceInImage,
               "TotalSizeOfStoredImages",
               TotalSizeInImage,
               coalesce("TotalSizeOfStoredImages", 0) - coalesce(TotalSizeInImage, 0) AS TotalSizeDelta,
               "NumberOfStoredImages",
               numberOfImagesInImage,
               coalesce("NumberOfStoredImages", 0) - coalesce(numberOfImagesInImage, 0) AS NumberOfImagesDelta,
               "TotalSizeOfThumbnails",
               totalSizeOfThumbnailsInImage,
               coalesce("TotalSizeOfThumbnails", 0) - coalesce(totalSizeOfThumbnailsInImage, 0) AS TotalSizeOfThumbnailsDelta
        -- selects with `with` execute on snapshots, so changes from the upsert aren't seen here.
        -- See https://www.postgresql.org/docs/11/queries-with.html
        From "CustomerStorage" AS y
        RIGHT OUTER JOIN cte ON y."Customer" = cte."Customer" AND y."Space" = cte."Space"
        WHERE   coalesce("TotalSizeOfStoredImages", 0) - coalesce(TotalSizeInImage, 0) != 0
        ORDER BY y."Customer",  cte."Customer", y."Space" , cte."Space";
        """)

    space_level_changes = cur.fetchall()

    records = {
        "customerChanges": customer_level_changes,
        "spaceChanges": space_level_changes}

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