-- Retention по дням:

WITH
	users_from_first_day AS
(
SELECT 
	toDate(l.time) AS day, 
	l.user_id,
	r.birthday
FROM
	simulator_20220420.feed_actions AS l
	LEFT JOIN
		(SELECT
			user_id,
			MIN(toDate(time)) AS birthday
		FROM
			simulator_20220420.feed_actions 
		GROUP BY
			user_id
		) AS r
	ON l.user_id = r.user_id
WHERE	
	birthday = (SELECT MIN(toDate(time)) FROM simulator_20220420.feed_actions)
)
SELECT
	day,
	COUNT(DISTINCT user_id) AS num_users
FROM	
	users_from_first_day
GROUP BY
	day