  {{
      config(materialized = 'table', tags = ['kpi'])
  }}

  select
      source,
      destination,
      source || ' -> ' || destination as route,
      count(*) as booking_count,
      round(avg(total_fare_bdt), 2) as average_total_fare_bdt
  from {{ ref('flight_prices') }}
  group by source, destination
  order by booking_count desc