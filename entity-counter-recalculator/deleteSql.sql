-- select customer images for deletion

WITH cte AS (
SELECT 'customer-images', x.count::bigint, x.customer
    FROM (SELECT count(*) as count, "Customer" as customer
      FROM "Images"
      GROUP BY customer) as x)
SELECT "Scope" as customer, count, "Next"
FROM "EntityCounters" as y
  LEFT OUTER JOIN cte ON cte.customer = y."Scope"::int
    where y."Type" = 'customer-images' and count is null and "Next" = 0
     order by "Scope"::int;

-- delete customer images

BEGIN;

WITH cte AS (
SELECT 'customer-images', x.count::bigint, x.customer
    FROM (SELECT count(*) as count, "Customer" as customer
      FROM "Images"
      GROUP BY customer) as x)
DELETE FROM "EntityCounters" as y
       USING cte
       WHERE y."Scope"::int = cte.customer and "Type" = 'customer-images' AND "Next" = 0 AND cte.count = 0
RETURNING "Scope" as customerDeleted;
ROLLBACK;

-- select space images for deletion

WITH cte AS (
SELECT 'space-images', x.space, x.count::bigint, x.customer
    FROM (SELECT count(*) as count, "Space" as space, "Customer" as customer
      FROM "Images"
      GROUP BY customer, space
      ORDER BY customer, space) as x)
SELECT "Customer", "space", "Scope", count, "Next"
FROM "EntityCounters" as y
  LEFT OUTER JOIN cte ON cte.customer = y."Customer" and cte.space::varchar = y."Scope"
    where y."Type" = 'space-images' AND "Next" = 0 AND count IS NULL
     order by "Customer", "Scope";

-- delete space images

BEGIN;

WITH cte AS (
SELECT 'space-images', x.space, x.count::bigint, x.customer
    FROM (SELECT count(*) as count, "Space" as space, "Customer" as customer
      FROM "Images"
      GROUP BY customer, space
      ORDER BY customer, space) as x)
DELETE FROM "EntityCounters" as y
       using cte
        WHERE y."Type" = 'space-images' and y."Next" = 0 and
              NOT EXISTS(SELECT FROM cte
              WHERE cte.customer = y."Customer" and cte.space::varchar = y."Scope")
    RETURNING "Customer", "Scope" as spaceDeleted;

ROLLBACK;