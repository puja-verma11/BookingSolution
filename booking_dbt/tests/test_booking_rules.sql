{{ config(severity='warn') }}

-- Custom test: rating must be between 0 and 5
-- Returns rows that FAIL the test (dbt fails if any rows returned)

SELECT
    booking_id,
    rating
FROM {{ ref('stg_bookings') }}
WHERE rating > 5
   OR rating < 0
