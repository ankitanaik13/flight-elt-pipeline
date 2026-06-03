-- Singular test: every non-cancelled flight must have a positive distance.
-- Returns violating rows; dbt fails the test if any are returned.
select *
from {{ ref('stg_flights') }}
where (distance is null or distance <= 0)
  and not is_cancelled
