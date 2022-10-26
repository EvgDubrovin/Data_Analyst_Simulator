-- Extract metrics from feed table

SELECT
	toDate(time) AS event_date, 
	user_id AS user, 
	gender, 
	age, 
	os, 
	countIf(action = 'like') AS likes, 
	countIf(action = 'views') AS views
FROM
	simulator_20220420.feed_actions
WHERE
	toDate(time) = yesterday()
GROUP BY
	event_date,  
	gender, 
	age, 
	os, 
	user
format TSVWithNames