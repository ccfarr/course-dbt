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

How many users do we have?  
A: 130

```
SELECT
  COUNT(1) AS number_of_rows -- 130
  ,COUNT(DISTINCT user_id) AS number_of_distinct_users -- 130
FROM dbt_chris_f.stg_users;
```

On average, how many orders do we receive per hour?
A:  TODO

```
SELECT
  hour_order_created
  ,CAST(AVG(number_of_orders) AS DECIMAL(5,2)) AS avg_number_of_orders_per_day
FROM (
  SELECT
    EXTRACT(HOUR FROM created_at) AS hour_order_created
    ,created_at::DATE AS created_at_date
    -- Confirmed that each row is a unique order
    ,COUNT(1) AS number_of_orders
  FROM dbt_chris_f.stg_orders
  GROUP BY 1, 2
  ORDER BY 1, 2
) AS number_of_orders_per_hour_day
GROUP BY 1;
```