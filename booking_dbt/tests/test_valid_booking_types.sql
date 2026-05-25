{{ config(severity='warn') }}

-- Warns if staging contains a type not in the approved list
-- To add new types: update the values list below
SELECT DISTINCT type
FROM {{ ref('stg_bookings') }}
WHERE type NOT IN ('attraction', 'hotel', 'restaurant', 'flight', 'unknown')
