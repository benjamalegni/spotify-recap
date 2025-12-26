{{ config(materialized='table') }}

-- plays by date
select
    date(played_at) as play_date,
    count(*) as total_plays,
    count(distinct artist_name) as unique_artists,
    count(distinct track_name) as unique_tracks,
    sum(ms_played) / 1000.0 / 60.0 as total_minutes_played,
    sum(ms_played) / 1000.0 / 60.0 / 60.0 as total_hours_played,
    avg(cast(ms_played as double)) / 1000.0 / 60.0 as avg_minutes_per_play
from {{ ref('stg_streaming_history') }}
where played_at is not null
group by date(played_at)