Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices


### Week 1:

**How many users do we have?**
Answer: 130

```
SELECT
  COUNT(DISTINCT user_id) AS number_of_distinct_users -- 130
FROM dbt_chris_f.stg_users;
```

**On average, how many orders do we receive per hour?**
Answer:
Please see this [thread](https://dbt-dth9192.slack.com/archives/C02HPAC9HHU/p1646844846725279)
which outlines various approaches. I answered the question in two different ways:

Approach 1: I counted the number of orders by hour (across the two days) and then computed an overall average. This yielded an overall average of 15.04 orders per hour.

```
SELECT
  CAST(AVG(number_of_orders) AS DECIMAL(5,2)) AS average_number_of_orders_per_hour
FROM (
  SELECT
    EXTRACT(HOUR FROM created_at) AS hour_order_created
    ,COUNT(DISTINCT order_id) AS number_of_orders
  FROM dbt_chris_f.stg_orders
  GROUP BY 1
) AS number_of_orders_per_hour;
```

Approach 2: I counted the number of orders by hour/day. I then computed the average across days for each hour.

| hour_order_created      | avg_number_of_daily_orders |
| ----------------------- | -------------------------- |
| 0                       | 7.50                       |
| 1                       | 7.00                       |
| 2                       | 5.50                       |
| 3                       | 6.00                       |
| 4                       | 6.50                       |
| 5                       | 5.00                       |
| 6                       | 6.50                       |
| 7                       | 6.00                       |
| 8                       | 6.00                       |
| 9                       | 7.50                       |
| 10                      | 12.50                      |
| 11                      | 12.50                      |
| 12                      | 6.00                       |
| 13                      | 7.00                       |
| 14                      | 9.00                       |
| 15                      | 8.00                       |
| 16                      | 9.50                       |
| 17                      | 7.50                       |
| 18                      | 7.50                       |
| 19                      | 4.00                       |
| 20                      | 10.00                      |
| 21                      | 7.50                       |
| 22                      | 6.00                       |
| 23                      | 10.00                      |

```
SELECT
  hour_order_created
  ,CAST(AVG(number_of_orders) AS DECIMAL(5,2)) AS avg_number_of_daily_orders
FROM (
  SELECT
    EXTRACT(HOUR FROM created_at) AS hour_order_created
    ,created_at::DATE AS created_at_date
    ,COUNT(DISTINCT order_id) AS number_of_orders
  FROM dbt_chris_f.stg_orders
  GROUP BY 1, 2
  ORDER BY 1, 2
) AS number_of_orders_per_hour_day
GROUP BY 1;
```

**On average, how long does an order take from being placed to being delivered?**
Answer: Assuming the date columns are in same time zone, the answer is 3 days 21:24:11 hours.

```
SELECT
  -- Confirmed created_at is not_null in dbt test
  SUM(CASE WHEN delivered_at IS NULL THEN 0 ELSE 1 END) AS number_of_deliveries -- 305
  ,AVG(delivered_at - created_at) AS average_time_to_deliver_order_in_days -- 3 days 21:24:11
  ,AVG(EXTRACT(epoch FROM (delivered_at - created_at)))/(60*60*24) AS double_check -- 3.8918 days
FROM
  dbt_chris_f.stg_orders;
```

**How many users have only made one purchase? Two purchases? Three+ purchases?**
Answer:
| number_of_orders        | number_of_users            |
| ----------------------- | -------------------------- |
| 1                       | 25                         |
| 2                       | 28                         |
| 3                       | 34                         |
| 4                       | 20                         |
| 5                       | 10                         |
| 6                       | 2                          |
| 7                       | 4                          |
| 8                       | 1                          |

```
SELECT
  number_of_orders,
  COUNT(DISTINCT user_id) AS number_of_users
FROM (
  SELECT
    user_id
    ,COUNT(DISTINCT order_id) AS number_of_orders
  FROM dbt_chris_f.stg_orders
  GROUP BY user_id
) AS number_of_orders_per_user
GROUP BY number_of_orders
ORDER BY number_of_orders ASC;
```

**On average, how many unique sessions do we have per hour?**
Answer: 39.46 unique sessions per hour

```
SELECT
  CAST(AVG(number_of_sessions) AS DECIMAL(5,2)) AS avg_number_of_sessions_per_hour -- 39.46
FROM (
  SELECT
    EXTRACT(HOUR FROM created_at) AS hour_created_at
    ,COUNT(DISTINCT session_id) AS number_of_sessions
  FROM dbt_chris_f.stg_events
  GROUP BY 1
) AS number_of_sessions_per_hour;
```
