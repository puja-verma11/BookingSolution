{{ config(materialized='table') }}
WITH cleaned AS (
    SELECT
        TRY_CAST(booking_id AS INT)          AS booking_id,
        LOWER(COALESCE(type, 'unknown'))     AS type,
        COALESCE(location, 'unknown')        AS location,
        name,
        category,
        TRY_CAST(rating AS FLOAT)            AS rating,
        CASE
            WHEN TRY_CAST(price AS INT) IS NULL THEN 0
            WHEN TRY_CAST(price AS INT) < 0 THEN 0
            ELSE TRY_CAST(price AS INT)
        END                                  AS price,
        loaded_at,
        source_file
    FROM {{ source('raw', 'bookings') }}
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY booking_id
            ORDER BY loaded_at DESC
        ) AS row_num
    FROM cleaned
)

SELECT
    booking_id,
    type,
    location,
    name,
    category,
    rating,
    price,
    loaded_at,
    source_file
FROM deduped
WHERE row_num = 1
