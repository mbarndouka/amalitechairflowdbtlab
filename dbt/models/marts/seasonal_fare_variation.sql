{{
      config(materialized = 'table', tags = ['kpi'])
  }}

  select
      case
          when is_peak_season then 'Peak'
          else 'Non-Peak'
      end as season_group,
      round(avg(total_fare_bdt), 2) as average_total_fare_bdt,
      count(*) as booking_count
  from {{ ref('flight_prices') }}
  group by season_group
  order by season_group