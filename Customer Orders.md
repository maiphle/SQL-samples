[Question Link](https://www.interviewquery.com/questions/customer-orders)

Write a query to identify customers who placed more than three transactions each in both 2019 and 2020.

Example:

Input:

transactions table

| Column     | Type     |
|------------|----------|
| id   | INTEGER  |
| user_id     | INTEGER  |
| created_at    | DATETIME  |
| product_id  | INTEGER  |
| quantity  | INTEGER |

users table

| Column     | Type     |
|------------|----------|
| id   | INTEGER  |
| name     | VARCHAR  |

Output:

| Column     | Type     |
|------------|----------|
| customer_name   | VARCHAR  |

## My solution

```
select distinct name as customer_name
from transactions t
join users u on t.user_id = u.id
where year(t.created_at) IN (2019, 2020)
group by u.id,  YEAR(t.created_at)
having count(t.id) > 3;
```

## Official solution


```
WITH transaction_counts AS (
SELECT u.id, 
name,
SUM(CASE WHEN YEAR(t.created_at)= '2019' THEN 1 ELSE 0 END) AS t_2019,
SUM(CASE WHEN YEAR(t.created_at)= '2020' THEN 1 ELSE 0 END) AS t_2020
FROM transactions t
JOIN users u
ON u.id = user_id
GROUP BY 1
HAVING t_2019 > 3 AND t_2020 > 3)

SELECT tc.name AS customer_name
FROM transaction_counts tc
```
