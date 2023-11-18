

Activity Table as table_1
day, host_id, session_id, live_seconds, host_region_id

Earnings Table as table_2
day, order_id, host_id, gifter_id, session_id, gift_value, host_region_id, gift_region_id

Area Table as table_3
id, region

Table Notes:
table_1 contains the activity of hosts for each day. For every active session you will have a unique host_id & session_id, that is, if a host is active multiple times in the day there will be one row of data for each session. Every active block is assigned a unique session_id. The total active time is returned as live_seconds and the id for the region the host belongs to is the host_region_id.

table_2 is the earnings table. For every gift received by the host you will have a unique order_id. The host_ID is the active host receiving the gift, the gifter_id is the user sending the gift, and the session_id is the active session of the host (as seen in table_1), the gift_amount is the gift value, host_region_id is the id of the region the host belongs do, and the gift_region_id is the id of the region the user sending the gift belongs to. 

table_3 stores the names of each region. The id column corresponds to the host and gift region ids in tables 1 & 2, and the region column is the name of the region. 

Question #1
Write a SQL query which returns the daily total live sessions, number of unique active hosts, total live hours, total gift value, and the percent of gift value received from the same region as the active host for the ‘SOUTHAMERICA’ region for each day between April 1, 2021 and April 20, 2021. 


```
SELECT 
    h.day,
    COUNT(DISTINCT h.session_id) AS total_live_sessions, 
    COUNT(DISTINCT h.host_id) AS unique_active_hosts, 
    SUM(h.live_seconds) / 3600 AS total_live_hours, 
    SUM(g.gift_value) AS total_gift_value, 
    CASE
        WHEN SUM(CASE WHEN g.gift_region_id = h.host_region_id THEN g.gift_value ELSE NULL END) / NULLIF(SUM(g.gift_value), 0) * 100 IS NULL THEN NULL
        ELSE SUM(CASE WHEN g.gift_region_id = h.host_region_id THEN g.gift_value ELSE NULL END) / NULLIF(SUM(g.gift_value), 0) * 100
    END AS perc_same_region_gift_value
FROM table_1 AS h 
LEFT JOIN table_2 AS g ON h.host_id = g.host_id AND h.session_id = g.session_id
LEFT JOIN table_3 AS r ON g.host_region_id = r.id
WHERE r.region = 'SOUTHAMERICA' 
    AND h.day BETWEEN '2021-04-01' AND '2021-04-20' 
GROUP BY h.day;

```

Question #2
Write a SQL query which returns the top 8 regions to receive gifts from the “AUSTRALIA” region between February 9, 2023 and March 17, 2023 sorted by total gift value high to low, the total number of unique hosts to receive gifts from the region, and the total number of unique gifters to send gifts. The first row of data would look like: Rank = 1, to_region (where the gifts went), total_gift_value, count_unique_hosts, count_unique_gifters.

```
WITH GiftSummary AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY SUM(g.gift_value) DESC) AS rank,
        r_to.region AS to_region,
        SUM(g.gift_value) AS total_gift_value,
        COUNT(DISTINCT g.host_id) AS count_unique_hosts, 
        COUNT(DISTINCT g.gifter_id) AS count_unique_gifters
    FROM table_2 AS g 
    LEFT JOIN table_3 AS r_from ON g.gift_region_id = r_from.id
    LEFT JOIN table_3 r_to ON g.host_region_id = r_to.id
    WHERE r_from.region = 'AUSTRALIA' 
        AND g.day BETWEEN '2023-02-09' AND '2023-03-17'
    GROUP BY g.gift_region_id, r_to.region
)

SELECT
    rank,
    to_region,
    total_gift_value,
    count_unique_hosts, 
    count_unique_gifters
FROM GiftSummary
WHERE rank <= 8
ORDER BY rank;

```
