{{
      config(
          materialized = 'table',
          tags = ['clean', 'fact']
      )
  }}

  with clean_rows as (
      select fp.*
      from {{ ref('stg_flight_prices') }} fp
      left join {{ ref('bd_flight_fare_row_audit') }} audit
          on fp.raw_row_number = audit.raw_row_number
      where audit.raw_row_number is null
  )

  select
      raw_row_number,
      airline,
      source,
      destination,
      source || ' -> ' || destination as route,
      departure_date_time,
      arrival_date_time,
      duration_hrs,
      stopovers,
      aircraft_type,
      flight_class,
      booking_source,
      base_fare_bdt,
      tax_surcharge_bdt,
      total_fare_bdt,
      seasonality,
      is_peak_season,
      days_before_departure
  from clean_rows