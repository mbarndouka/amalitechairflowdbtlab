{{
      config(materialized = 'table', tags = ['kpi'])
  }}

  select
      airline,
      count(*) as booking_count
  from {{ ref('flight_prices') }}
  group by airline
  order by booking_count desc
