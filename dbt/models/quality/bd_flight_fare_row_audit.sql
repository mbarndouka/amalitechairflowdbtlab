{{
    config(
        materialized = 'table',
        tags = ['quality','validation']
    )
 }}

with flight_prices as (
    select * from {{ ref('stg_flight_prices') }}
),
airports as (
    select
        upper(trim(airport_code)) as airport_code,
        trim(city_name) as city_name
    from {{ ref('bd_airport')}}
),
validated as (
    select
        fp.*,
        source_airport.airport_code is not null as source_airport_is_valid,
        destination_airport.airport_code is not null as destination_airport_is_valid
    from flight_prices fp
    left join airports source_airport
        on fp.source = source_airport.airport_code
    left join airports destination_airport
        on fp.destination = destination_airport.airport_code
)

select
    raw_row_number,
    airline,
    source,
    destination,
    base_fare_bdt,
    tax_surcharge_bdt,
    source_total_fare_bdt,
    calculated_total_fare_bdt,
    total_fare_bdt,

    airline is null as missing_airline,
    source is null as missing_source,
    destination is null as missing_destination,
    base_fare_bdt is null as missing_base_fare,
    tax_surcharge_bdt is null as missing_tax_surcharge,
    total_fare_bdt is null as missing_total_fare,

    base_fare_bdt < 0 as negative_base_fare,
    tax_surcharge_bdt < 0 as negative_tax_surcharge,
    total_fare_bdt < 0 as negative_total_fare,

    not source_airport_is_valid as invalid_source_airport,
    not destination_airport_is_valid as invalid_destination_airport,

    abs(calculated_total_fare_bdt - source_total_fare_bdt) > 0.01
        as corrected_total_fare
from validated
where
    airline is null
    or source is null
    or destination is null
    or base_fare_bdt is null
    or tax_surcharge_bdt is null
    or total_fare_bdt is null
    or base_fare_bdt < 0
    or tax_surcharge_bdt < 0
    or total_fare_bdt < 0
    or not source_airport_is_valid
    or not destination_airport_is_valid
    or abs(calculated_total_fare_bdt - source_total_fare_bdt) > 0.01