{{ config(materialized='table') }}

-- plays by platform/device
select
    platform,
    count(*) as total_plays,
    count(distinct artist_name) as unique_artists,
    sum(ms_played) / 1000.0 / 60.0 / 60.0 as total_hours_played,
    round(
        count(*) * 100.0 / (select count(*) from {{ ref('stg_streaming_history') }}),
        2
    ) as percentage_of_total_plays
from {{ ref('stg_streaming_history') }}
where platform is not null
group by platform