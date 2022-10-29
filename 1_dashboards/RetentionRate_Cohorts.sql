-- Calculating retention rate for feed users

WITH
	users_days AS 
(
SELECT	
	toDate(l.time) as day, 
	l.user_id,
	r.birthday,
	DATEDIFF(day - birthday) AS days_distance
FROM
	simulator_20220420.feed_actions AS l 
LEFT JOIN
	(SELECT	
		user_id,
		MIN(toDate(time)) as birthday
	FROM
		simulator_20220420.feed_actions
	GROUP BY
		user_id) AS r
ON l.user_id = r.user_id
)

SELECT	
	birthday AS cohort,
	days_distance,
	COUNT(DISTINCT user_id) AS num_of_users
FROM
	users_days
GROUP BY
	cohort,
	days_distance