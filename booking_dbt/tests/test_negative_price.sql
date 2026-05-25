{{ config(severity='warn') }}

-- Custom test: price must be zero or positive
-- Returns rows that FAIL the test (dbt fails if any rows returned)

SELECT
    booking_id,
    price
FROM {{ ref('stg_bookings') }}
WHERE price < 0
