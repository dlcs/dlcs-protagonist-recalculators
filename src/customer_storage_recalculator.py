import psycopg2
from psycopg2 import extras

from logzero import logger
from app.customer_storage_recalculator_settings import (CONNECTION_STRING, DRY_RUN, ENABLE_CLOUDWATCH_INTEGRATION,
                                                        CLOUDWATCH_SPACE_IMAGE_SIZE_DIFFERENCE_METRIC_NAME,
                                                        CLOUDWATCH_SPACE_IMAGE_NUMBER_DIFFERENCE_METRIC_NAME,
                                                        CLOUDWATCH_SPACE_THUMBNAIL_SIZE_DIFFERENCE_METRIC_NAME,
                                                        CONNECTION_TIMEOUT, APP_VERSION, AWS_CONNECTION_STRING_LOCATION, LOCALSTACK, REGION,
                                                        LOCALSTACK_ADDRESS)
from app.aws_factory import get_aws_client
from app.database import connect_to_postgres, get_connection_config


def begin_cleanup():
    connection_info = get_connection_config(connection_string=CONNECTION_STRING,
                                            aws_connection_string_location=AWS_CONNECTION_STRING_LOCATION,
                                            region=REGION,
                                            localstack=LOCALSTACK,
                                            localstack_address=LOCALSTACK_ADDRESS)
    conn = connect_to_postgres(connection_info=connection_info, connection_timeout=CONNECTION_TIMEOUT)
    records = __run_sql(conn)

    if ENABLE_CLOUDWATCH_INTEGRATION:
        logger.info("setting cloudwatch metrics")
        cloudwatch = get_aws_client(resource_type="cloudwatch", localstack=LOCALSTACK,
                                    region=REGION, localstack_address=LOCALSTACK_ADDRESS)
        set_cloudwatch_metrics(records, cloudwatch, connection_info)

    conn.close()

    return records


def set_cloudwatch_metrics(records, cloudwatch, connection_info):
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

    # gather differences in space images
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

    cur.execute("""
        UPDATE "CustomerStorage"
        SET "NumberOfStoredImages"    = numberOfImages,
            "TotalSizeOfStoredImages" = totalImageSize,
            "TotalSizeOfThumbnails"   = totalThumbnailSize,
            "LastCalculated"          = now()
        FROM (SELECT "Customer",
                     SUM("NumberOfStoredImages")    AS numberOfImages,
                     SUM("TotalSizeOfStoredImages") AS totalImageSize,
                     SUM("TotalSizeOfThumbnails")   AS totalThumbnailSize
              FROM "CustomerStorage"
              WHERE "Space" != 0
              GROUP BY "Customer"
              ORDER BY "Customer") AS vals
        WHERE "CustomerStorage"."Customer" = vals."Customer";
    """)

    records = {
        "spaceChanges": space_level_changes
    }

    logger.info(records)

    if DRY_RUN:
        logger.info(f"DRY RUN ENABLED.  Changes have not been committed to the database")
    else:
        conn.commit()

    cur.close()

    return records


if __name__ == "__main__":
    begin_cleanup()


def handler(event, context):
    logger.debug("calling handler...")
    return begin_cleanup()