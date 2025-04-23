import psycopg2
from psycopg2 import extras

from logzero import logger
from app.entity_counter_recalculator_settings import (DRY_RUN, ENABLE_CLOUDWATCH_INTEGRATION,
                                                      CLOUDWATCH_SPACE_DELETE_METRIC_NAME,
                                                      CLOUDWATCH_SPACE_DIFFERENCE_METRIC_NAME,
                                                      APP_VERSION, REGION,
                                                      CONNECTION_TIMEOUT, CONNECTION_STRING)
from app.aws_factory import get_aws_client
from app.database import connect_to_postgres, get_connection_config


def begin_cleanup():
    connection_info = get_connection_config(CONNECTION_STRING)
    conn = connect_to_postgres(connection_info=connection_info,connection_timeout=CONNECTION_TIMEOUT)
    records = __run_sql(conn)

    if ENABLE_CLOUDWATCH_INTEGRATION:
        logger.info("setting cloudwatch metrics")
        cloudwatch = get_aws_client(resource_type="cloudwatch", region=REGION)
        set_cloudwatch_metrics(records, cloudwatch, connection_info)

    conn.close()

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
                    'Value': "entity_counter_recalculator"
                }]

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
            'Unit': 'Count',
            'Value': space_delta
        },
        {
            'MetricName': CLOUDWATCH_SPACE_DELETE_METRIC_NAME,
            'Dimensions': dimensions,
            'Unit': 'Count',
            'Value': space_deletes_needed
        }
    ])

    try:
        logger.debug(f"updating cloudwatch metrics - {metric_data}")
        cloudwatch.put_metric_data(MetricData=metric_data,
                                   Namespace='Protagonist Recalculator')
        return metric_data
    except Exception as e:
        logger.error(f"Error posting to cloudwatch: {e}")
        raise e


def __run_sql(conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # update space images
    cur.execute("""
        WITH cte AS (
        SELECT 'space-images', x.space, x.count::bigint, x.customer
            FROM (SELECT count(*) as count, "Space" as space, "Customer" as customer
              FROM "Images"
              GROUP BY customer, space
              ORDER BY customer, space) as x), ins as
                  (
        INSERT INTO "EntityCounters" ("Type", "Scope", "Next", "Customer")
        SELECT 'space-images', cte.space, cte.count::bigint, cte.customer
        FROM cte
        ON CONFLICT ("Type", "Scope", "Customer")
        DO UPDATE SET ("Type", "Scope", "Next", "Customer")  =
            ROW (excluded."Type", excluded."Scope", excluded."Next", excluded."Customer")
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

    cur.execute("""
        UPDATE "EntityCounters" as ec
        SET "Next" = totalImages
        FROM (select SUM("Next") AS totalImages,
                     "Customer"
            from "EntityCounters"
            where "Type" = 'space-images'
            group by "Customer") AS vals
        WHERE ec."Type" = 'customer-images' and ec."Scope"::INT = vals."Customer";
    """)

    records = {
        "spaceImages": space_image_changes}

    logger.info(records)

    if DRY_RUN:
        logger.info(f"DRY RUN ENABLED.  Changes have not been committed to the database")
    else:
        conn.commit()

    cur.close()

    return records


if __name__ == "__main__":
    begin_cleanup()
