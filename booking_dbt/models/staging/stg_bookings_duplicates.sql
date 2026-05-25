WITH cleaned AS (
    SELECT
        TRY_CAST(booking_id AS INT)          AS booking_id,
        LOWER(COALESCE(type, 'unknown'))     AS type,
        COALESCE(location, 'unknown')        AS location,
        name,
        category,
        TRY_CAST(rating AS FLOAT)            AS rating,
        COALESCE(TRY_CAST(price AS INT), 0)  AS price,
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

-- only duplicate records for auditing
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
WHERE row_num > 1
