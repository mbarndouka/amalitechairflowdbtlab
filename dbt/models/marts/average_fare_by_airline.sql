{{
      config(materialized = 'table', tags = ['kpi'])
  }}

  select
      airline,
      round(avg(total_fare_bdt), 2) as average_total_fare_bdt,
      count(*) as booking_count
  from {{ ref('flight_prices') }}
  group by airline
  order by average_total_fare_bdt desc