# Weekly Project Assignments

[Week 1 Assignment](#week-1)  
[Week 2 Assignment](#week-2)  
[Week 3 Assignment](#week-3)  
[Week 4 Assignment](#week-4)  

## Week 1

**How many users do we have?**
Answer: 130

```sql
SELECT
  COUNT(DISTINCT user_id) AS number_of_distinct_users -- 130
FROM dbt_chris_f.stg_users;
```

**On average, how many orders do we receive per hour?**
Answer:
Please see this [thread](https://dbt-dth9192.slack.com/archives/C02HPAC9HHU/p1646844846725279)
which outlines various approaches. I answered the question in two different ways:

Approach 1: I counted the number of orders by hour (across the two days) and then computed an overall average. This yielded an overall average of 15.04 orders per hour.

```sql
SELECT
  CAST(AVG(number_of_orders) AS DECIMAL(5,2)) AS average_number_of_orders_per_hour -- 15.04
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

```sql
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

```sql
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

```sql
SELECT
  number_of_orders
  ,SUM(1) AS number_of_users
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

```sql
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

## Week 2

### (Part 1) Models

**What is our user repeat rate?**  
Answer: The repeat rate is **79.84%**

```Repeat Rate = Users who purchased 2 or more times / users who purchased```

```sql
SELECT
  SUM(CASE WHEN number_of_orders > 1 THEN 1 ELSE 0 END) AS number_of_users_who_purchased_two_or_more -- 99
  ,SUM(CASE WHEN number_of_orders > 0 THEN 1 ELSE 0 END) AS number_of_users_who_purchased -- 124
  ,CAST(100.0 * SUM(CASE WHEN number_of_orders > 1 THEN 1 ELSE 0 END) /
    SUM(CASE WHEN number_of_orders > 0 THEN 1 ELSE 0 END) AS DECIMAL(5,2)) AS repeat_rate -- 79.84
FROM (
  SELECT
    user_id
    ,COUNT(DISTINCT order_id) AS number_of_orders
  FROM dbt_chris_f.stg_orders
  GROUP BY user_id
) AS number_of_orders_per_user;
```

**What are good indicators of a user who will likely purchase again? What about indicators of users who are likely NOT to purchase again? If you had more data, what features would you want to look into to answer this question?**  
Answer: In my work, I focus on attributes related to a user's first order, like whether she received a promotion or not. See below where I find that users who received a promotion for their first order were more likely to be a repeat customer:

| first_order_has_promotion | number_of_users  | repeat_rate |
| ------------------------- | ---------------- | ----------- |
| NULL (i.e.TOTAL)          | 124              | 79.84       |
| 0                         | 110              | 78.18       |
| 1                         | 14               | 92.86       | 

```
SELECT
  first_order_has_promotion
  ,COUNT(1) AS number_of_users
  ,CAST(100*AVG(is_repeat_customer) AS DECIMAL(5,2)) AS repeat_rate
FROM dbt_chris_f.agg_users_orders
GROUP BY CUBE(1);
```

Note, the nifty trick that since `is_repeat_customer` takes on either `1 - true` and `0 - false`, an `AVG()` aggregate function applied to this column yields a proportion.

**More stakeholders are coming to us for data, which is great! But we need to get some more models created before we can help. Create a marts folder, so we can organize our models, with the following subfolders for business units: Core, Marketing, and Product.**  
Answer: Done

**Within each marts folder, create intermediate models and dimension/fact models.**  
Answer: Done, where in the `marketing` and `product` folders, I authored so-called aggregate tables (`agg_`) to compute user- and session-focused metrics.

**Explain the marts models you added. Why did you organize the models in the way you did?**  
Answer: In my `core` folder, I authored two fact tables (`fact_events` and `fact_orders`) where I joined in columns from tables like addresses and promotions. I chose to join in these columns and present a wider, denormalized table for my analyst users, so they would not have to keep up with foreign keys and complicated joins. I only focused on two dimension tables (`dim_products` and `dim_users`) for now.

In my `marketing` and `product` folders, I authored aggregate tables to support analyst queries. Ideally, the aggregate tables would be used primarily by analysts, and they could go to the "star schema" in the `core` folder as needed.

Of note, models in `staging` and `intermediate` folders ("stepping stone" models) are materialized as views. All other models are materialized as tables to make querying faster for our analysts.

Lastly, I chose to have only the models in the `core` folder use `stg_` models. The `marketing` and `product` models only `ref(...)` in models from the `core` folder. The idea was to centralize transformations as much as possible to avoid repeated code.

**Use the dbt docs to visualize your model DAGs to ensure the model layers make sense**  
Answer:

![lineage_graph](lineage_graph.jpg)

### (Part 2) Tests

**We added some more models and transformed some data! Now we need to make sure they’re accurately reflecting the data. Add dbt tests into your dbt project on your existing models from Week 1, and new models from the section above**  

**What assumptions are you making about each model? (i.e. why are you adding each test?)**  
Answer: I put `unique` tests on all columns that were used in joins to prevent any unintended duplication of rows. I put `accepted_values` tests on selected columns to support references in `CASE` statements in my aggregate models. You'll see that all folders have a `schema.yml` file with at least one test.

**Did you find any “bad” data as you added and ran tests on your models? How did you go about either cleaning the data in the dbt model or adjusting your assumptions/tests?**  
Answer: I derived the `order_cost` using `int_orders_derived_cost` and found that the numbers checked out. I did not find any additional errors in the data.

**Apply these changes to your github repo**  
Answer: Done

**Your stakeholders at Greenery want to understand the state of the data each day. Explain how you would ensure these tests are passing regularly and how you would alert stakeholders about bad data getting through.**  
Answer: One could execute a `dbt test` each day to document if any test generaterd a `failure` or a `warning`. If issues were found, upstream data owners could be notified.

## Week 3

### PART 1: Create new models to answer the first two questions:

**What is our overall conversion rate?**  
Answer: 62.46% (I confirmed `number_of_events_checkout` is either `0` or `1`.)
```sql
-- Overall conversion rate
SELECT
  CAST(100*AVG(number_of_events_checkout) AS DECIMAL(5,2)) AS overall_conversion_rate -- 62.46
FROM dbt_chris_f.agg_sessions_events;
```

**What is our conversion rate by product?**  
Answer: Top five products with highest conversion rate (please see model `agg_sessions_products`):

| product_name              | product_conversion_rate |
| ------------------------- | ----------------------- |
| String of Pearls          | 60.94                   |
| Arrow Head                | 55.56                   |
| Cactus                    | 54.55                   |
| ZZ Plant                  | 53.97                   |
| Bamboo                    | 53.73                   |

```sql
-- product conversion rate, top 5
SELECT
  product_name
  ,CAST(100.0 * SUM(was_purchased) / SUM(was_viewed) AS DECIMAL(5,2)) AS product_conversion_rate
FROM dbt_chris_f.agg_sessions_products
GROUP BY 1
ORDER BY 2 DESC
LIMIT 5;
```

### PART 2: Create a macro to simplify part of a model(s)

Please see `agg_users_orders`:

```sql
{% set event_types = ["shipped", "preparing", "delivered"] %}
...
    SELECT
        {% for event_type in event_types %}
        ,SUM(CASE WHEN order_status = 'event_type' THEN 1 ELSE 0 END) AS number_of_orders_{{event_type}}
        {% endfor %}
```

### PART 3: Post-Hook

See the following in `dbt_project.yml`:

```
models:
  greenery:
    ...
  post-hook:
    - "GRANT SELECT ON {{ this }} TO reporting"

on-run-end:
  - "GRANT USAGE ON SCHEMA {{ schema }} TO reporting"
```

### Part 4: Packages

See existing use of `dbt_utils` package for the "primary key" test below:

```
  - name: stg_order_items
    description: Order items table
    columns:
      - name: order_id
        tests:
          - not_null
      - name: product_id
        tests:
          - not_null
    tests:
    - dbt_utils.unique_combination_of_columns:
        combination_of_columns:
          - order_id
          - product_id
```

### Part 5: Updated DAG

![lineage_graph_week3](lineage_graph_week3.png)

## Week 4

### Part 1: dbt Snapshots

Please see `snapshots/snp_orders.sql` where I performed the required steps and confirmed the insertion of 3 records after the second run of `dbt snapshot`.

### Part 2: Modeling challenge

Please see `marts/marketing/agg_funnel.sql` which produced the overall funnel metrics:

| level                                     | number_of_sessions | % of Level 1 |
| ----------------------------------------- | ------------------ | ------------ |
| Level 1: page_view, add_to_cart, checkout | 578                | 100.00       |
| Level 2: add_to_cart, checkout            | 467                | 80.80        |
| Level 3: checkout                         | 361                | 62.46        | 

Exposures:
Please see `models/exposures.yml` and updated lineage graph with exposure highlighted at right:

![lineage_graph_week4](dag_with_exposure.jpg)

### Part 3: Reflection questions (3A. dbt next steps for you)

**Prompt: if your organization is using dbt, what are 1-2 things you might do differently / recommend to your organization based on learning from this course?**

* [dbt source freshness](https://docs.getdbt.com/reference/resource-properties/freshness)  
* https://github.com/lightdash/dbt2looker  
* [run_results.json](https://docs.getdbt.com/reference/artifacts/run-results-json)  
* [Using dbt artifacts to track project performance](https://discourse.getdbt.com/t/using-dbt-artifacts-to-track-project-performance/1873)  
* [Analytics on your analytics](https://www.youtube.com/watch?v=kqyxHCHH0d4)  
* [dbt snapshots](https://docs.getdbt.com/docs/building-a-dbt-project/snapshots)  
* [dbt ls](https://docs.getdbt.com/reference/commands/list)  
* [Hooks & Operations](https://docs.getdbt.com/docs/building-a-dbt-project/hooks-operations)  
* Model Layers  
* [pgweb](https://sosedoff.github.io/pgweb/)  
* Gitpod  
* [Exposures](https://docs.getdbt.com/docs/building-a-dbt-project/exposures)    