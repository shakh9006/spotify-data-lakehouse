{{
    config(
        materialized: 'incremental',
        unique_key: 'event_id',
        incremental_strategy: 'merge',
        on_schema_change: 'sync_all_columns'
    )
}}

WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'spotify_events')}}

    {% if is_incremental() %}
        WHERE timestamp > (select max(timestamp) from {{ this }})
    {% endif %}
),

cleaned AS (
    SELECT
        event_id,
        user_id,
        artist_id,
        artist_name,
        song_id,
        song_name,
        song_year,
        event_type,
        device,
        country,
        timestamp

        CURRENT_TIMESTAMP AS loaded_date,
        DATE(timestamp) AS event_date,
        HOUR(timestamp) AS event_hour
    FROM source
    WHERE 1=1
        AND event_id IS NOT NULL
        AND user_id IS NOT NULL
        AND artist_id IS NOT NULL
        AND artist_name IS NOT NULL
        AND song_id IS NOT NULL
        AND song_name IS NOT NULL
        AND song_year IS NOT NULL
        AND event_type IS NOT NULL
        AND device IS NOT NULL
        AND country IS NOT NULL
        AND timestamp IS NOT NULL
),

deduplicated AS (
    SELECT *
    FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY timestamp DESC) AS rn
        FROM cleaned
    )
    WHERE rn = 1
)

SELECT
    event_id,
    user_id,
    artist_id,
    artist_name,
    song_id,
    song_name,
    song_year,
    event_type,
    device,
    country,
    timestamp,
    loaded_date,
    event_date,
    event_hour
FROM deduplicated