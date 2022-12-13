{{ config(materialized="table") }}

with
    epidemiology as (select * from {{ ref("base_epidemiology") }}),

    demographic as (select * from {{ ref("base_demographic") }}),

    economy as (select * from {{ ref("base_economy") }}),

    index as (select * from {{ ref("base_index") }}),

    epidemiology_join as (
        select
            epidemiology.date,
            epidemiology.location_key,
            iff(
                epidemiology.new_confirmed = 'NaN', 0, epidemiology.new_confirmed
            ) as new_confirmed,
            iff(
                epidemiology.new_deceased = 'NaN', 0, epidemiology.new_deceased
            ) as new_deceased,
            iff(
                epidemiology.new_recovered = 'NaN', 0, epidemiology.new_recovered
            ) as new_recovered,
            iff(
                epidemiology.new_tested = 'NaN', 0, epidemiology.new_tested
            ) as new_tested,
            iff(
                epidemiology.cumulative_confirmed = 'NaN',
                0,
                epidemiology.cumulative_confirmed
            ) as cumulative_confirmed,
            iff(
                epidemiology.cumulative_deceased = 'NaN',
                0,
                epidemiology.cumulative_deceased
            ) as cumulative_deceased,
            iff(
                epidemiology.cumulative_recovered = 'NaN',
                0,
                epidemiology.cumulative_recovered
            ) as cumulative_recovered,
            iff(
                epidemiology.cumulative_tested = 'NaN',
                0,
                epidemiology.cumulative_tested
            ) as cumulative_tested,
            economy.gdp_usd,
            economy.gdp_per_capita_usd,
            economy.human_capital_index,
            demographic.population,
            demographic.population_male,
            demographic.population_female,
            demographic.population_rural,
            demographic.population_urban,
            demographic.population_largest_city,
            demographic.population_clustered,
            demographic.population_density,
            demographic.human_development_index,
            demographic.population_age_00_09,
            demographic.population_age_10_19,
            demographic.population_age_20_29,
            demographic.population_age_30_39,
            demographic.population_age_40_49,
            demographic.population_age_50_59,
            demographic.population_age_60_69,
            demographic.population_age_70_79,
            demographic.population_age_80_and_older,
            index.place_id,
            index.wikidata_id,
            index.country_code,
            index.country_name,
            index.locality_code,
            index.locality_name,
            index.datacommons_id,
            index.subregion1_code,
            index.subregion1_name,
            index.subregion2_code,
            index.subregion2_name,
            index.aggregation_level,
            index.iso_3166_1_alpha_2,
            index.iso_3166_1_alpha_3
        from epidemiology
        left join economy on epidemiology.location_key = economy.location_key
        left join demographic on epidemiology.location_key = demographic.location_key
        left join index on epidemiology.location_key = index.location_key
    )

select *
from epidemiology_join
