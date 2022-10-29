-- Retention в ленте новостей за последние 20 дней

WITH cte AS 
  (
  SELECT 
      user_id, toDate(time) as day,
      birthday, source
  FROM
      simulator_20220420.feed_actions as l 
  LEFT JOIN
    (
    SELECT 
      user_id, MIN(toDate(time)) AS birthday, MAX(source) as source
    FROM
      simulator_20220420.feed_actions
    GROUP BY
      user_id
    ) as r 
  USING user_id
  WHERE birthday BETWEEN (today()-20) AND today()
  )
SELECT
  toString(birthday) as birthday, toString(day) AS day, source, 
  COUNT(DISTINCT user_id) as num_of_users
FROM 
  cte 
GROUP BY 
  birthday, day, source
ORDER BY 
  birthday, day