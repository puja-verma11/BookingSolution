-- Final business-ready model
-- Only clean, validated data from staging

WITH stg AS (
    SELECT * FROM {{ ref('stg_bookings') }}
),

final AS (
    SELECT
        booking_id,
        type,
        location,
        name,
        category,

        -- enforce valid rating range 0-5
        CASE
            WHEN rating > 5 THEN NULL
            WHEN rating < 0 THEN NULL
            ELSE rating
        END AS rating,

        -- enforce positive price
        CASE
            WHEN price < 0 THEN 0
            ELSE price
        END AS price,

        loaded_at,
        source_file

    FROM stg
    -- exclude records with unknown type
    WHERE type != 'unknown'
)

SELECT * FROM final
