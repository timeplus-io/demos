
-- most active users in the last day
SELECT
  message:by AS username,
  count() AS total_posts,
  count_if(message:type = 'story') AS stories,
  count_if(message:type = 'comment') AS comments,
  max(to_datetime(to_int64(message:time))) AS last_activity
FROM table(hn.hn_post)
WHERE _tp_time > now() - INTERVAL 1 DAY and username != ''
GROUP BY username
ORDER BY total_posts DESC
LIMIT 10;

-- post type distribution in the last hour
SELECT
  message:type AS post_type,
  count() AS count
FROM table(hn.hn_post)
WHERE _tp_time > now() - INTERVAL 1 HOUR
GROUP BY post_type
ORDER BY count DESC;