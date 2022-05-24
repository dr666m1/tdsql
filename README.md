# Test Driven SQL
Tdsql is a minimum test flamework for sql.
You can replace any part of sql and check if the result is as expected.
You can define test cases as yaml file.

## Install
Currently, only bigquery is supported.

```bash
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
      data: |
        SELECT * FROM UNNEST([
          STRUCT('2020-01-01' AS dt, 100 AS id),
          STRUCT('2020-01-01', 100),
          STRUCT('2020-01-01', 200)
        ])
      master: |
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
-- ./hello-world.sql
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
FROM data INNER JOIN master USING(id)
GROUP BY 1, 2
```

Then, run this command.
You'll see an error message.

```sh
tdsql
```

Fix `hello-world.sql` and run `tdsql` again,
you won't see any error message this time.

```diff
- FROM data INNER JOIN master USING(id)
+ FROM data LEFT JOIN master USING(id)
```

Quite simple, isn't it?

## Examples
Heavily documented sample codes are [here](./sample).

## Feedback
If you find any bugs, please feel free to create an issue.
