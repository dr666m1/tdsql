-- ./hello-world.sql

WITH data AS (

  -- Use this pattern to replace several lines
  -- tdsql-start: data
  SELECT dt, id
  FROM `data_table`
  -- tdsql-end: data

), master AS (

  -- Use this pattern to replace a single line
  SELECT id, category
  FROM `master_table` -- tdsql-line: master

)

-- tdsql-start: main
SELECT
  dt,
  category,
  COUNT(*) AS cnt
FROM data left JOIN master USING(id)
GROUP BY 1, 2
-- tdsql-end: main
