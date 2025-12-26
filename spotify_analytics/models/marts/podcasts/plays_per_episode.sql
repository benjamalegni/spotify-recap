{{ config(materialized='table') }}

-- plays per podcast episode
select
    episode_name,
    episode_show_name as show_name,
    count(*) as total_plays,
    sum(ms_played) / 1000.0 / 60.0 as total_minutes_played,
    avg(cast(ms_played as double)) / 1000.0 / 60.0 as avg_minutes_per_play,
    sum(case when skipped = true then 1 else 0 end) as skipped_count,
    round(
        sum(case when skipped = true then 1 else 0 end) * 100.0 / count(*),
        2
    ) as skip_rate_percent,
    min(played_at) as first_played,
    max(played_at) as last_played
from {{ ref('stg_streaming_history') }}
where episode_name is not null
group by episode_name, episode_show_name