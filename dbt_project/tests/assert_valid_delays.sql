-- Singular test: no flight may have an arrival or departure delay > 1500 minutes.
-- Returns rows that violate the constraint; dbt fails the test if any rows are returned.
select *
from {{ ref('stg_flights') }}
where arr_delay_minutes > 1500
   or dep_delay_minutes > 1500
