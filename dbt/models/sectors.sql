WITH raw_data AS (
  SELECT
    ROUND(
      cmc_categories."timestamp"
    ) AS "timestamp",
    CAST(json_array_elements(DATA) AS json) AS json_row
  FROM
    cmc_categories)
  SELECT
    to_timestamp(raw_data."timestamp") AS "timestamp",
    raw_data.json_row ->> 'title' AS NAME,
    CAST (
      raw_data.json_row ->> 'marketCap' AS DOUBLE PRECISION
    ) AS marketCap,
    CAST (
      raw_data.json_row ->> 'marketVolume' AS DOUBLE PRECISION
    ) AS marketVolume,
    {{ ref('sector_tag') }}.tag
  FROM
    raw_data
  LEFT JOIN {{ ref('sector_tag') }} ON raw_data.json_row ->> 'title' = {{ ref('sector_tag') }}.Sector
