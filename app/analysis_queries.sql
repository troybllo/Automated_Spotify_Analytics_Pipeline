-- Most Played tracks
SELECT
  track_name,
  artist_names,
  COUNT(*) as play_count,
  MIN(played_at) as first_played,
  MAX(played_at) as last_played,
FROM raw_data.recently_played
GROUP BY track_name, artist_names
ORDER BY play_count DESC 
LIMIT 20;


-- Listening time patterns
SELECT 
  EXTRACT(HOUR FROM played_at) as hour_of_day
  COUNT(*) as plays,
  ROUND(AVG(duration_ms / 1000.0 / 60),2) as avg_duration_minutes
FROM raw_data.recently_played
GROUP BY hour_of_day
ORDER BY hour_of_day;

-- Top genres from your top artists
WITH genre_split AS (
  SELECT
    artist_name,
    UNNEST(STRING_TO_ARRAY(generes, ',')) as genre,
    popularity,
    time_range
  FROM raw_data.top_artists
  WHERE genres != ''
)
SELECT 
  TRIM(genre) as genre,
  COUNT(DISTINCT artist_name) as artist_count,
  ROUND(AVG(popularity), 2) as avg_popularity
FROM genre_split
GROUP BY TRIM(genre)
ORDER BY artist_count DESC
LIMIT 20;


