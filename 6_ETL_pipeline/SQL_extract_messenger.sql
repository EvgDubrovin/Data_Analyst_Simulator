-- Extract metrics from messenger

SELECT
	event_date, 
	user, 
	gender, 
	age, 
	os, 
	messages_sent, 
	messages_received, 
	users_sent, 
	users_received
FROM
	(
	SELECT		
		toDate(time) AS event_date, 
		user_id AS user, 
		gender, 
		age, 
		os, 
		COUNT(reciever_id) as messages_sent,
        uniqExact(reciever_id) as users_sent
	FROM
		simulator_20220420.message_actions
	WHERE
		toDate(time) = yesterday()
	GROUP BY
		event_date,
		user, 
		gender, 
		age, 
		os
	) AS sent
	
FULL JOIN
	(
	SELECT		
		toDate(time) AS event_date, 
		reciever_id AS user, 
		gender, 
		age, 
		os, 
		COUNT(user_id) as messages_received,
        uniqExact(user_id) as users_received
	FROM
		simulator_20220420.message_actions
	WHERE
		toDate(time) = yesterday()
	GROUP BY
		event_date,
		user, 
		gender, 
		age, 
		os
	) AS received
	
ON sent.user = received.user
format TSVWithNames