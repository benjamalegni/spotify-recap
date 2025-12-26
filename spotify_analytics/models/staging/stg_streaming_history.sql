{{ config(materialized='view') }}

-- Staging model: clean and cast types from the seed
-- Includes both tracks and podcasts - marts will filter as needed
select
    cast(ts as timestamptz) as played_at,
    platform,
    cast(ms_played as bigint) as ms_played,
    conn_country,
    ip_addr,
    master_metadata_track_name as track_name,
    master_metadata_album_artist_name as artist_name,
    master_metadata_album_album_name as album_name,
    spotify_track_uri,
    episode_name,
    episode_show_name,
    spotify_episode_uri,
    reason_start,
    reason_end,
    cast(shuffle as boolean) as shuffle,
    cast(skipped as boolean) as skipped,
    cast(offline as double) as offline,
    cast(offline_timestamp as bigint) as offline_timestamp,
    cast(incognito_mode as boolean) as incognito_mode,
    source_file,
    -- Helper field to identify content type
    case 
        when master_metadata_track_name is not null then 'track'
        when episode_name is not null then 'podcast'
        else 'other'
    end as content_type
from {{ source('seeds', 'streaming_history_all') }}
where (master_metadata_track_name is not null or episode_name is not null)
  and cast(ms_played as bigint) > 2000  -- Filter out very short or incomplete plays
