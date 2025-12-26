{{ config(materialized='table') }}

-- podcasts listening habits: hour of day, day of week
select
    extract(hour from played_at) as hour_of_day,
    extract(dow from played_at) as day_of_week,
    case extract(dow from played_at)
        when 0 then 'Sunday'
        when 1 then 'Monday'
        when 2 then 'Tuesday'
        when 3 then 'Wednesday'
        when 4 then 'Thursday'
        when 5 then 'Friday'
        when 6 then 'Saturday'
    end as day_name,
    count(*) as total_plays,
    sum(ms_played) / 1000.0 / 60.0 / 60.0 as total_hours_played
from {{ ref('stg_streaming_history') }}
where episode_name is not null and played_at is not null
group by extract(hour from played_at), extract(dow from played_at)