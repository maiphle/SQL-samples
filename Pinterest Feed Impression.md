Let’s say you work at Pinterest. On Pinterest’s internal database, the table pins contains an identifier number and creation date/time of pins (content such as pictures or videos) that a user posted on his profile.

The table event_log is a log table containing data about all actions on pins.

Possible action types are:

"View" - Logged when pins appear on a user’s screen.
"Engagement" - Logged when actions such as saves or pin-clicks occur.
Write a query to determine the percentage of users that have viewed at least one content within seven days from the content being posted and with at least one engagement recorded on any pin, regardless of when it occurred.

Example:

Input:

pins table

Columns	Type
pin_id	INTEGER
created_at	DATETIME
event_log table

Columns	Type
event_id	INTEGER
pin_id	INTEGER
user_id	INTEGER
action_type	VARCHAR
action_date	DATETIME
Output:

Columns	Type
percent_of_users	FLOAT


## My solution: 
```
SELECT
    COUNT(DISTINCT user_id) / (SELECT COUNT(DISTINCT user_id) FROM event_log) AS percent_of_users
FROM
    event_log el
JOIN
    pins p ON el.pin_id = p.pin_id
WHERE
    action_type = 'View'
    AND action_date BETWEEN p.created_at AND DATE_ADD(p.created_at, INTERVAL 7 DAY)
    AND EXISTS ( // returns TRUE if the subquery returns one or more records.
        SELECT 1 // we are only interested in checking the existence of a record, and the value '1' is a placeholder.
        FROM event_log
        WHERE pin_id = el.pin_id
          AND action_type = 'Engagement'
    );
```

## Official solution:

```
WITH users_who_viewed_within_a_week  AS 
(
  SELECT user_id, max(viewed_within_a_week) AS viewed_within_a_week // Viewed at least one pin within a week
  FROM 
  (
    SELECT user_id, created_at, action_date, IF(DATEDIFF(action_date,created_at) <= 7, 1, 0) AS viewed_within_a_week
    FROM event_log a
    JOIN pins b on a.pin_id = b.pin_id
	WHERE action_type= 'view' 
   ) x
  GROUP BY user_id HAVING viewed_within_a_week = 1
),
users_who_reacted AS 
(
SELECT DISTINCT user_id  FROM event_log WHERE action_type = 'Engagement' // Made an engagement on any pin, regardless of when it occurred
)
SELECT (SELECT COUNT(1) FROM users_who_viewed_within_a_week x
JOIN users_who_reacted y ON x.user_id = y.user_id)
/
(SELECT count(distinct user_id) AS total_number_of_users FROM event_log)
AS percent_of_users
```
