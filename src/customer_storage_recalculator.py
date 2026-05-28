import psycopg2
from psycopg2 import extras

from logzero import logger
from app.customer_storage_recalculator_settings import (CONNECTION_STRING, DRY_RUN, ENABLE_CLOUDWATCH_INTEGRATION,
                                                        CLOUDWATCH_SPACE_IMAGE_SIZE_DIFFERENCE_METRIC_NAME,
                                                        CLOUDWATCH_SPACE_IMAGE_NUMBER_DIFFERENCE_METRIC_NAME,
                                                        CLOUDWATCH_SPACE_THUMBNAIL_SIZE_DIFFERENCE_METRIC_NAME,
                                                        CLOUDWATCH_SPACE_ADJUNCT_SIZE_DIFFERENCE_METRIC_NAME,
                                                        CLOUDWATCH_SPACE_ADJUNCT_NUMBER_DIFFERENCE_METRIC_NAME,
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
    space_total_adjunct_size_delta = 0
    space_total_adjunct_number_delta = 0

    for key in records["spaceChanges"]:
        logger.info(f"Space delta - {key}")
        space_total_image_size_delta += abs(key["TotalSizeDelta"])
        space_total_image_number_delta += abs(key["NumberOfImagesDelta"])
        space_total_thumbnail_size_delta += abs(key["TotalSizeOfThumbnailsDelta"])
        space_total_adjunct_size_delta += abs(key["TotalAdjunctSizeDelta"])
        space_total_adjunct_number_delta += abs(key["NumberOfAdjunctsDelta"])

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
        },
        {
            'MetricName': CLOUDWATCH_SPACE_ADJUNCT_SIZE_DIFFERENCE_METRIC_NAME,
            'Dimensions': dimensions,
            'Unit': 'Bytes',
            'Value': space_total_adjunct_size_delta
        },
        {
            'MetricName': CLOUDWATCH_SPACE_ADJUNCT_NUMBER_DIFFERENCE_METRIC_NAME,
            'Dimensions': dimensions,
            'Unit': 'Count',
            'Value': space_total_adjunct_number_delta
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

    # Step 1: aggregate ImageStorage and Adjuncts per customer/space and upsert into CustomerStorage.
    # Three CTEs build up the fresh totals:
    #   img_cte   — sums Size, ThumbnailSize, and AdjunctSize from ImageStorage
    #   adj_cte   — counts hosted adjuncts (Origin IS NOT NULL/non-empty) from the Adjuncts table,
    #               extracting Customer and Space from the AssetId string ({customer}/{space}/{id})
    #   cte       — joins the two, so every customer/space row has both image and adjunct metrics
    # The UPSERT inserts new rows for customer/space pairs not yet in CustomerStorage,
    # or updates existing rows when any total has changed.
    # ON CONFLICT targets the partial unique index on (Customer, Space) WHERE Space IS NOT NULL,
    # which covers all real spaces including space 0 (stub assets). The NULL-space aggregate
    # row is never written here — it is derived in step 3.
    # The final SELECT returns the pre-upsert snapshot joined against the new CTE values so
    # we can report deltas. CTEs execute on a snapshot before any DML in the same statement,
    # so CustomerStorage reads here reflect the state *before* the upsert.
    # See https://www.postgresql.org/docs/11/queries-with.html
    cur.execute("""
        -- Aggregate image/thumbnail/adjunct sizes from ImageStorage per customer/space.
        WITH img_cte AS (
            SELECT "Customer", "Space",
                   SUM("Size")          AS "TotalSizeInImageStorageTable",
                   COUNT("Id")          AS "NumberOfImagesInImageStorageTable",
                   SUM("ThumbnailSize") AS "TotalSizeOfThumbnailsInImageStorageTable",
                   SUM("AdjunctSize")   AS "TotalAdjunctSizeInImageStorageTable"
            FROM "ImageStorage"
            GROUP BY "Customer", "Space"
        ),
        -- Count hosted adjuncts per customer/space.
        -- AssetId is stored as '{customer}/{space}/{id}', so split_part extracts the components.
        -- Only adjuncts with a non-empty Origin are hosted (i.e. stored by DLCS).
        adj_cte AS (
            SELECT CAST(split_part("AssetId", '/', 1) AS integer) AS "Customer",
                   CAST(split_part("AssetId", '/', 2) AS integer) AS "Space",
                   COUNT(*) AS "NumberOfAdjunctsInAdjunctsTable"
            FROM "Adjuncts"
            WHERE "Origin" IS NOT NULL AND "Origin" != ''
            GROUP BY 1, 2
        ),
        -- Combine image and adjunct aggregates. LEFT JOIN so spaces with no hosted adjuncts
        -- still appear with a zero adjunct count.
        cte AS (
            SELECT img_cte."Customer",
                   img_cte."Space",
                   img_cte."TotalSizeInImageStorageTable",
                   img_cte."NumberOfImagesInImageStorageTable",
                   img_cte."TotalSizeOfThumbnailsInImageStorageTable",
                   img_cte."TotalAdjunctSizeInImageStorageTable",
                   COALESCE(adj_cte."NumberOfAdjunctsInAdjunctsTable", 0) AS "NumberOfAdjunctsInAdjunctsTable"
            FROM img_cte
            LEFT JOIN adj_cte
                   ON img_cte."Customer" = adj_cte."Customer"
                  AND img_cte."Space"    = adj_cte."Space"
            ORDER BY img_cte."Customer", img_cte."Space"
        ),
        -- Upsert the fresh aggregates into CustomerStorage for all non-null spaces.
        ins AS (
            INSERT INTO "CustomerStorage"
                   ( "Customer", "StoragePolicy", "NumberOfStoredImages", "TotalSizeOfStoredImages",
                     "TotalSizeOfThumbnails", "NumberOfStoredAdjuncts", "TotalSizeOfStoredAdjuncts",
                     "LastCalculated", "Space" )
                   SELECT cte."Customer", 'default', cte."NumberOfImagesInImageStorageTable", cte."TotalSizeInImageStorageTable",
                          cte."TotalSizeOfThumbnailsInImageStorageTable", cte."NumberOfAdjunctsInAdjunctsTable", cte."TotalAdjunctSizeInImageStorageTable",
                          current_timestamp, cte."Space"
                    FROM cte
                   ON CONFLICT ("Customer", "Space") WHERE "Space" IS NOT NULL
                   DO UPDATE SET
                       "Customer"                = excluded."Customer",
                       "StoragePolicy"           = excluded."StoragePolicy",
                       "NumberOfStoredImages"    = excluded."NumberOfStoredImages",
                       "TotalSizeOfStoredImages" = excluded."TotalSizeOfStoredImages",
                       "TotalSizeOfThumbnails"   = excluded."TotalSizeOfThumbnails",
                       "NumberOfStoredAdjuncts"  = excluded."NumberOfStoredAdjuncts",
                       "TotalSizeOfStoredAdjuncts" = excluded."TotalSizeOfStoredAdjuncts",
                       "LastCalculated"          = excluded."LastCalculated",
                       "Space"                   = excluded."Space"
                   RETURNING *
        )
        -- Return only rows where something changed, comparing the pre-upsert CustomerStorage
        -- snapshot against the freshly aggregated values.
        SELECT y."Customer",
               cte."Customer" AS "CustomerInImageStorageTable",
               y."Space",
               cte."Space" AS "SpaceInImageStorageTable",
               "TotalSizeOfStoredImages",
               "TotalSizeInImageStorageTable",
               coalesce("TotalSizeOfStoredImages", 0) - coalesce("TotalSizeInImageStorageTable", 0) AS "TotalSizeDelta",
               "NumberOfStoredImages",
               "NumberOfImagesInImageStorageTable",
               coalesce("NumberOfStoredImages", 0) - coalesce("NumberOfImagesInImageStorageTable", 0) AS "NumberOfImagesDelta",
               "TotalSizeOfThumbnails",
               "TotalSizeOfThumbnailsInImageStorageTable",
               coalesce("TotalSizeOfThumbnails", 0) - coalesce("TotalSizeOfThumbnailsInImageStorageTable", 0) AS "TotalSizeOfThumbnailsDelta",
               "TotalSizeOfStoredAdjuncts",
               "TotalAdjunctSizeInImageStorageTable",
               coalesce("TotalSizeOfStoredAdjuncts", 0) - coalesce("TotalAdjunctSizeInImageStorageTable", 0) AS "TotalAdjunctSizeDelta",
               "NumberOfStoredAdjuncts",
               "NumberOfAdjunctsInAdjunctsTable",
               coalesce("NumberOfStoredAdjuncts", 0) - coalesce("NumberOfAdjunctsInAdjunctsTable", 0) AS "NumberOfAdjunctsDelta"
        FROM "CustomerStorage" AS y
        RIGHT OUTER JOIN cte ON y."Customer" = cte."Customer" AND y."Space" = cte."Space"
        WHERE coalesce("TotalSizeOfStoredImages", 0)       - coalesce("TotalSizeInImageStorageTable", 0)        != 0
           OR coalesce("NumberOfStoredImages", 0)          - coalesce("NumberOfImagesInImageStorageTable", 0)   != 0
           OR coalesce("TotalSizeOfThumbnails", 0)         - coalesce("TotalSizeOfThumbnailsInImageStorageTable", 0) != 0
           OR coalesce("TotalSizeOfStoredAdjuncts", 0)     - coalesce("TotalAdjunctSizeInImageStorageTable", 0) != 0
           OR coalesce("NumberOfStoredAdjuncts", 0)        - coalesce("NumberOfAdjunctsInAdjunctsTable", 0)     != 0
        ORDER BY y."Customer", cte."Customer", y."Space", cte."Space";
        """)

    space_level_changes = cur.fetchall()

    # Step 2: zero out CustomerStorage rows for spaces that no longer have any ImageStorage records.
    # The upsert in step 1 only touches spaces that currently exist in ImageStorage, so a space
    # whose last image was deleted would be silently skipped and retain stale non-zero totals.
    # Adjunct columns are included: if all images are gone, adjuncts cannot exist either
    # (adjuncts are always associated with an image). Any residual adjunct values are also zeroed.
    # Only non-null spaces are considered — the NULL aggregate row is never written directly.
    # The old values are captured via the to_zero CTE (which reads the pre-update snapshot) so
    # they can be reported as deltas alongside the changes from step 1.
    cur.execute("""
        -- Identify non-null spaces with no remaining ImageStorage records.
        WITH to_zero AS (
            SELECT "Customer", "Space",
                   "NumberOfStoredImages", "TotalSizeOfStoredImages", "TotalSizeOfThumbnails",
                   "NumberOfStoredAdjuncts", "TotalSizeOfStoredAdjuncts"
            FROM "CustomerStorage"
            WHERE "Space" IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM "ImageStorage"
                WHERE "ImageStorage"."Customer" = "CustomerStorage"."Customer"
                  AND "ImageStorage"."Space" = "CustomerStorage"."Space"
              )
        ),
        -- Zero out the identified rows and stamp LastCalculated.
        -- Covers both non-zero rows (actual changes) and already-zero rows (e.g. space 0 with
        -- no images/adjuncts) that are skipped by the step 1 upsert but still need LastCalculated
        -- updated. The final SELECT filters to only rows with non-zero deltas for reporting.
        zeroed AS (
            UPDATE "CustomerStorage"
            SET "NumberOfStoredImages"      = 0,
                "TotalSizeOfStoredImages"   = 0,
                "TotalSizeOfThumbnails"     = 0,
                "NumberOfStoredAdjuncts"    = 0,
                "TotalSizeOfStoredAdjuncts" = 0,
                "LastCalculated"            = current_timestamp
            FROM to_zero
            WHERE "CustomerStorage"."Customer" = to_zero."Customer"
              AND "CustomerStorage"."Space"    = to_zero."Space"
        )
        -- Return the old (pre-zero) values as deltas so they feed into CloudWatch metrics.
        -- Rows already at zero are excluded — their delta is 0 so nothing meaningful to report.
        SELECT "Customer",
               "Space",
               "TotalSizeOfStoredImages",
               0 AS "TotalSizeInImageStorageTable",
               "TotalSizeOfStoredImages"   AS "TotalSizeDelta",
               "NumberOfStoredImages",
               0 AS "NumberOfImagesInImageStorageTable",
               "NumberOfStoredImages"      AS "NumberOfImagesDelta",
               "TotalSizeOfThumbnails",
               0 AS "TotalSizeOfThumbnailsInImageStorageTable",
               "TotalSizeOfThumbnails"     AS "TotalSizeOfThumbnailsDelta",
               "TotalSizeOfStoredAdjuncts",
               0 AS "TotalAdjunctSizeInImageStorageTable",
               "TotalSizeOfStoredAdjuncts" AS "TotalAdjunctSizeDelta",
               "NumberOfStoredAdjuncts",
               0 AS "NumberOfAdjunctsInAdjunctsTable",
               "NumberOfStoredAdjuncts"    AS "NumberOfAdjunctsDelta"
        FROM to_zero
        WHERE "NumberOfStoredImages"    != 0
           OR "TotalSizeOfStoredImages" != 0
           OR "TotalSizeOfThumbnails"   != 0
           OR "NumberOfStoredAdjuncts"  != 0
           OR "TotalSizeOfStoredAdjuncts" != 0;
        """)

    zeroed_space_changes = cur.fetchall()
    space_level_changes = list(space_level_changes) + list(zeroed_space_changes)

    # Step 3: roll up all per-space totals into the NULL-space aggregate row for each customer.
    # Space IS NULL is the customer-level aggregate (formerly space 0 prior to the
    # StopSpaceZeroCustomerStorage migration). All non-null spaces — including space 0 which
    # now tracks stub assets — are included in the sum.
    cur.execute("""
        UPDATE "CustomerStorage" AS cs
        SET "NumberOfStoredImages"      = numberOfImages,
            "TotalSizeOfStoredImages"   = totalImageSize,
            "TotalSizeOfThumbnails"     = totalThumbnailSize,
            "NumberOfStoredAdjuncts"    = numberOfAdjuncts,
            "TotalSizeOfStoredAdjuncts" = totalAdjunctSize,
            "LastCalculated"            = current_timestamp
        FROM (
            -- Sum every real space (non-null) per customer to produce the customer-wide totals.
            SELECT "Customer",
                   SUM("NumberOfStoredImages")      AS numberOfImages,
                   SUM("TotalSizeOfStoredImages")   AS totalImageSize,
                   SUM("TotalSizeOfThumbnails")     AS totalThumbnailSize,
                   SUM("NumberOfStoredAdjuncts")    AS numberOfAdjuncts,
                   SUM("TotalSizeOfStoredAdjuncts") AS totalAdjunctSize
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