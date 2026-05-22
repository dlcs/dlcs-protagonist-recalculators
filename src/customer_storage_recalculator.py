import psycopg2
from psycopg2 import extras

from logzero import logger
from app.customer_storage_recalculator_settings import (CONNECTION_STRING, DRY_RUN, ENABLE_CLOUDWATCH_INTEGRATION,
                                                        CLOUDWATCH_SPACE_IMAGE_SIZE_DIFFERENCE_METRIC_NAME,
                                                        CLOUDWATCH_SPACE_IMAGE_NUMBER_DIFFERENCE_METRIC_NAME,
                                                        CLOUDWATCH_SPACE_THUMBNAIL_SIZE_DIFFERENCE_METRIC_NAME,
                                                        CONNECTION_TIMEOUT, APP_VERSION, REGION)
from app.aws_factory import get_aws_client
from app.database import connect_to_postgres, get_connection_config


def run_cleanup():
    logger.info("Running customer storage recalculator")
    connection_info = get_connection_config(CONNECTION_STRING)
    conn = connect_to_postgres(connection_info=connection_info, connection_timeout=CONNECTION_TIMEOUT)
    records = __run_sql(conn)

    if ENABLE_CLOUDWATCH_INTEGRATION:
        logger.info("CloudWatch metrics enabled")
        cloudwatch = get_aws_client(resource_type="cloudwatch", region=REGION)
        set_cloudwatch_metrics(records, cloudwatch, connection_info)

    conn.close()
    logger.info("Customer storage recalculator complete")

    return records


def set_cloudwatch_metrics(records, cloudwatch, connection_info):
    metric_data = []
    dimensions = [{
                    'Name': "DATABASE_NAME",
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
        logger.info(f"Space delta - {key}")
        space_total_image_size_delta += abs(key["totalsizedelta"])
        space_total_image_number_delta += abs(key["numberofimagesdelta"])
        space_total_thumbnail_size_delta += abs(key["totalsizeofthumbnailsdelta"])

    metric_data.extend([
        {
            'MetricName': CLOUDWATCH_SPACE_IMAGE_SIZE_DIFFERENCE_METRIC_NAME,
            'Dimensions': dimensions,
            'Unit': 'Bytes',
            'Value': space_total_image_size_delta
        },
        {
            'MetricName': CLOUDWATCH_SPACE_IMAGE_NUMBER_DIFFERENCE_METRIC_NAME,
            'Dimensions': dimensions,
            'Unit': 'Count',
            'Value': space_total_image_number_delta
        },
        {
            'MetricName': CLOUDWATCH_SPACE_THUMBNAIL_SIZE_DIFFERENCE_METRIC_NAME,
            'Dimensions': dimensions,
            'Unit': 'Bytes',
            'Value': space_total_thumbnail_size_delta
        }
    ])

    try:
        logger.debug(f"Publishing CloudWatch metrics - {metric_data}")
        cloudwatch.put_metric_data(MetricData=metric_data,
                                   Namespace='Protagonist Recalculator')
        return metric_data
    except Exception as e:
        logger.exception(f"Error publishing to CloudWatch")
        raise e


def __run_sql(conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Step 1: aggregate ImageStorage per customer/space and upsert into CustomerStorage.
    # The CTE produces fresh totals from the source-of-truth ImageStorage table.
    # The UPSERT inserts new rows for customer/space pairs not yet in CustomerStorage,
    # or updates existing rows when totals have changed.
    # ON CONFLICT targets the partial unique index on (Customer, Space) WHERE Space IS NOT NULL,
    # which covers all real spaces including space 0 (stub assets). The NULL-space aggregate
    # row is never written here — it is derived in step 3.
    # The final SELECT returns the pre-upsert snapshot joined against the new CTE values so
    # we can report deltas. CTEs execute on a snapshot before any DML in the same statement,
    # so CustomerStorage reads here reflect the state *before* the upsert.
    # See https://www.postgresql.org/docs/11/queries-with.html
    cur.execute("""
        -- Aggregate current totals from ImageStorage for every customer/space combination.
        WITH cte AS (
            SELECT "Customer", "Space",
                   SUM("Size")          AS TotalSizeInImage,
                   COUNT("Id")          AS numberOfImagesInImage,
                   SUM("ThumbnailSize") AS totalSizeOfThumbnailsInImage
            FROM "ImageStorage"
            GROUP BY "Customer", "Space"
            ORDER BY "Customer", "Space"
        ),
        -- Upsert the fresh aggregates into CustomerStorage for all non-null spaces.
        ins AS (
            INSERT INTO "CustomerStorage"
                   ( "Customer", "StoragePolicy", "NumberOfStoredImages", "TotalSizeOfStoredImages", "TotalSizeOfThumbnails", "LastCalculated", "Space" )
                   SELECT cte."Customer", 'default', cte.numberOfImagesInImage, cte.TotalSizeInImage, cte.totalSizeOfThumbnailsInImage, current_timestamp, cte."Space"
                    FROM cte
                   ON CONFLICT ("Customer", "Space") WHERE "Space" IS NOT NULL
                   DO UPDATE SET
                       "Customer" = excluded."Customer",
                       "StoragePolicy" = excluded."StoragePolicy",
                       "NumberOfStoredImages" = excluded."NumberOfStoredImages",
                       "TotalSizeOfStoredImages" = excluded."TotalSizeOfStoredImages",
                        "TotalSizeOfThumbnails" = excluded."TotalSizeOfThumbnails",
                       "LastCalculated" = excluded."LastCalculated",
                       "Space" = excluded."Space"
                   RETURNING *
        )
        -- Return only rows where something changed, comparing the pre-upsert CustomerStorage
        -- snapshot against the freshly aggregated ImageStorage values.
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
        FROM "CustomerStorage" AS y
        RIGHT OUTER JOIN cte ON y."Customer" = cte."Customer" AND y."Space" = cte."Space"
        WHERE coalesce("TotalSizeOfStoredImages", 0) - coalesce(TotalSizeInImage, 0) != 0
        ORDER BY y."Customer", cte."Customer", y."Space", cte."Space";
        """)

    space_level_changes = cur.fetchall()

    # Step 2: zero out CustomerStorage rows for spaces that no longer have any ImageStorage records.
    # The upsert in step 1 only touches spaces that currently exist in ImageStorage, so a space
    # whose last image was deleted would be silently skipped and retain stale non-zero totals.
    # Only non-null spaces are considered — the NULL aggregate row is never written directly.
    # The old values are captured via the to_zero CTE (which reads the pre-update snapshot) so
    # they can be reported as deltas alongside the changes from step 1.
    cur.execute("""
        -- Identify non-null spaces that have non-zero totals but no remaining ImageStorage records.
        WITH to_zero AS (
            SELECT "Customer", "Space",
                   "NumberOfStoredImages", "TotalSizeOfStoredImages", "TotalSizeOfThumbnails"
            FROM "CustomerStorage"
            WHERE "Space" IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM "ImageStorage"
                WHERE "ImageStorage"."Customer" = "CustomerStorage"."Customer"
                  AND "ImageStorage"."Space" = "CustomerStorage"."Space"
              )
              AND ("NumberOfStoredImages" != 0 OR "TotalSizeOfStoredImages" != 0 OR "TotalSizeOfThumbnails" != 0)
        ),
        -- Zero out the identified rows.
        zeroed AS (
            UPDATE "CustomerStorage"
            SET "NumberOfStoredImages" = 0,
                "TotalSizeOfStoredImages" = 0,
                "TotalSizeOfThumbnails" = 0,
                "LastCalculated" = current_timestamp
            FROM to_zero
            WHERE "CustomerStorage"."Customer" = to_zero."Customer"
              AND "CustomerStorage"."Space" = to_zero."Space"
        )
        -- Return the old (pre-zero) values as deltas so they feed into CloudWatch metrics.
        SELECT "Customer",
               "Space",
               "TotalSizeOfStoredImages",
               0 AS TotalSizeInImage,
               "TotalSizeOfStoredImages" AS TotalSizeDelta,
               "NumberOfStoredImages",
               0 AS numberOfImagesInImage,
               "NumberOfStoredImages" AS NumberOfImagesDelta,
               "TotalSizeOfThumbnails",
               0 AS totalSizeOfThumbnailsInImage,
               "TotalSizeOfThumbnails" AS TotalSizeOfThumbnailsDelta
        FROM to_zero;
        """)

    zeroed_space_changes = cur.fetchall()
    space_level_changes = list(space_level_changes) + list(zeroed_space_changes)

    # Step 3: roll up all per-space totals into the NULL-space aggregate row for each customer.
    # Space IS NULL is the customer-level aggregate (formerly space 0 prior to the
    # StopSpaceZeroCustomerStorage migration). All non-null spaces — including space 0 which
    # now tracks stub assets — are included in the sum.
    cur.execute("""
        UPDATE "CustomerStorage" AS cs
        SET "NumberOfStoredImages"    = numberOfImages,
            "TotalSizeOfStoredImages" = totalImageSize,
            "TotalSizeOfThumbnails"   = totalThumbnailSize,
            "LastCalculated"          = now()
        FROM (
            -- Sum every real space (non-null) per customer to produce the customer-wide totals.
            SELECT "Customer",
                   SUM("NumberOfStoredImages")    AS numberOfImages,
                   SUM("TotalSizeOfStoredImages") AS totalImageSize,
                   SUM("TotalSizeOfThumbnails")   AS totalThumbnailSize
            FROM "CustomerStorage"
            WHERE "Space" IS NOT NULL
            GROUP BY "Customer"
            ORDER BY "Customer"
        ) AS vals
        -- Write the aggregated totals into the NULL-space row for this customer.
        WHERE cs."Customer" = vals."Customer" AND "Space" IS NULL;
    """)

    records = {
        "spaceChanges": space_level_changes
    }

    logger.info(records)

    if DRY_RUN:
        logger.info(f"DRY RUN ENABLED. Changes have not been committed to the database")
    else:
        conn.commit()

    cur.close()

    return records


if __name__ == "__main__":
    run_cleanup()
