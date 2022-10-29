-- Новые, старые и ушедшие пользователи

 
SELECT this_week, previous_week, -uniq(user_id) as num_users, status FROM

(SELECT user_id, 
-- все недели, когда пользователь совершал действие в ленте
groupUniqArray(toMonday(toDate(time))) as weeks_visited, 
-- для каждого пользователя записываем в новую строку следующую неделю (из посещенных)
addWeeks(arrayJoin(weeks_visited), +1) this_week, 
-- присваиваем статус 'retained', если пользователь совершил действие в ленте на следующую неделю; 'gone' - если нет
if(has(weeks_visited, this_week) = 1, 'retained', 'gone') as status, 
-- для каждого пользователя записываем в новую строку текущую неделю (из посещенных)
addWeeks(this_week, -1) as previous_week
FROM simulator_20220420.feed_actions
GROUP BY user_id)

WHERE status = 'gone'
GROUP BY this_week, previous_week, status
HAVING this_week != addWeeks(toMonday(today()), +1) -- не рассматриваем следующую неделю от текущей даты (т.к. это абсурд) 

UNION ALL

SELECT this_week, previous_week, toInt64(uniq(user_id)) as num_users, status FROM

(SELECT user_id, 
groupUniqArray(toMonday(toDate(time))) as weeks_visited, 
arrayJoin(weeks_visited) this_week, 
-- если пользователь совершал действия в ленте на прошлой неделе, статус 'retained'; иначе - 'new'
if(has(weeks_visited, addWeeks(this_week, -1)) = 1, 'retained', 'new') as status, 
addWeeks(this_week, -1) as previous_week
FROM simulator_20220420.feed_actions
GROUP BY user_id)

GROUP BY this_week, previous_week, status