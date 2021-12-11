WITH
  rawdata AS (
  SELECT
    ARRAY(
    SELECT
      x
    FROM
      UNNEST(JSON_EXTRACT_ARRAY(DATA,
          "$")) x ) data_row,
    _timestamp AS `timestamp`
  FROM
    `default`.`categories_change` )
SELECT
  timestamp,
  JSON_VALUE(data_row,
    '$.title') AS name,
  CAST(JSON_VALUE(data_row,
    '$.marketCap') as NUMERIC) AS marketCap,
  CAST(JSON_VALUE(data_row,
    '$.marketVolume') as NUMERIC) AS marketVolume,
    sector_tag.tag AS tag
FROM
  rawdata,
  rawdata.data_row
LEFT JOIN {{ ref('sector_tag') }} ON JSON_VALUE(data_row, '$.title') = sector_tag.Sector
