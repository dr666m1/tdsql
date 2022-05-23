# Test Driven SQL
WIP

## Install
Currently, only bigquery is supported.

```
pip install tdsql[bigquery]
```

## Quick start
Save these files in your working directory.

```yaml
# ./tdsql.yaml
database: bigquery
tests:
  - filepath: ./hello-world.sql
    replace:
      - data: |
          SELECT * FROM UNNEST([
            STRUCT('2020-01-01' AS dt, 100 AS id),
            STRUCT('2020-01-01', 100),
            STRUCT('2020-01-01', 200)
          ])
      - master: |
          FROM (
            SELECT 100 AS id, 1 AS category
          )
    expected: |
      SELECT * FROM UNNEST([
        STRUCT('2020-01-01' AS dt, 1 AS category, 2 AS cnt),
        STRUCT('2020-01-01', NULL, 1)
      ])
```

```sql
# ./hello-world.sql
WITH data AS (
  -- tdsql-start: data
  SELECT dt, id
  FROM `data_table`
  -- tdsql-end: data
), master AS (
  SELECT id, category
  FROM `master_table` -- tdsql-line: master
)
SELECT
  dt,
  category,
  COUNT(*) AS cnt
FROM data INNER JOIN master
GROUP BY 1
;
```

Then, run this command.
You'll see an error message.

```sh
tdsql
```

Fix the sql and run `tdsql` again,
you won't see any error message this time.

```diff
# ./hello-world.sql

- FROM data INNER JOIN master
+ FROM data LEFT JOIN master
```

Quite simple, isn't it?
