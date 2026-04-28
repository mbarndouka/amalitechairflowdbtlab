 {{
      config(
          materialized = 'table',
          tags = ['staging', 'validation']
      )
  }}

  with raw as (
      select
          row_number() over (
              order by departure_date_time, airline, source, destination
          ) as raw_row_number,

          nullif(trim(airline), '') as airline,
          upper(nullif(trim(source), '')) as source,
          nullif(trim(source_name), '') as source_name,
          upper(nullif(trim(destination), '')) as destination,
          nullif(trim(destination_name), '') as destination_name,

          departure_date_time,
          arrival_date_time,
          duration_hrs,

          nullif(trim(stopovers), '') as stopovers,
          nullif(trim(aircraft_type), '') as aircraft_type,
          nullif(trim(class), '') as flight_class,
          nullif(trim(booking_source), '') as booking_source,

          base_fare_bdt,
          tax_surcharge_bdt,
          total_fare_bdt as source_total_fare_bdt,

          nullif(trim(seasonality), '') as seasonality,
          days_before_departure
      from {{ source('raw', 'raw_flight_prices') }}
  ),

  validated as (
      select
          raw.*,

          case
              when base_fare_bdt is not null
               and tax_surcharge_bdt is not null
               and base_fare_bdt >= 0
               and tax_surcharge_bdt >= 0
                  then base_fare_bdt + tax_surcharge_bdt
          end as calculated_total_fare_bdt,

          case
              when lower(seasonality) in ('eid', 'winter holidays') then true
              else false
          end as is_peak_season
      from raw
  )

  select
      raw_row_number,
      airline,
      source,
      source_name,
      destination,
      destination_name,
      departure_date_time,
      arrival_date_time,
      duration_hrs,
      stopovers,
      aircraft_type,
      flight_class,
      booking_source,
      base_fare_bdt,
      tax_surcharge_bdt,

      case
          when source_total_fare_bdt is null then calculated_total_fare_bdt
          when abs(source_total_fare_bdt - calculated_total_fare_bdt) > 0.01
              then calculated_total_fare_bdt
          else source_total_fare_bdt
      end as total_fare_bdt,

      source_total_fare_bdt,
      calculated_total_fare_bdt,
      seasonality,
      is_peak_season,
      days_before_departure
  from validated