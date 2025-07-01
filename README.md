## aggregation functions

SELECT
p.product_category_name,
COUNT(oi.order_id) AS total_orders,
SUM(oi.price) AS total_revenue,
AVG(oi.price) AS average_price,
MIN(oi.price) AS min_price,
MAX(oi.price) AS max_price
FROM
  olist_order_items AS oi
JOIN
  olist_products AS p ON oi.product_id = p.product_id
GROUP BY
  p.product_category_name
ORDER BY
  total_revenue DESC;
  
 ## windows functions

SELECT 
  oi.order_id, 
  o.customer_id, 
  p.product_id, 
  c.customer_state
FROM olist_order_items oi
JOIN olist_orders o ON oi.order_id = o.order_id
JOIN olist_customers c ON o.customer_id = c.customer_id
JOIN olist_products p ON oi.product_id = p.product_id
LIMIT 10;

SELECT *
FROM (
  SELECT
    order_id,
    payment_type,
    payment_installments,
    payment_value,
    ROW_NUMBER() OVER (
      PARTITION BY payment_type
      ORDER BY payment_value DESC
    ) AS row_num,
    RANK() OVER (
      PARTITION BY payment_type
      ORDER BY payment_value DESC
    ) AS rank_in_type,
    DENSE_RANK() OVER (
      PARTITION BY payment_type
      ORDER BY payment_value DESC
    ) AS dense_rank_in_type,
    NTILE(4) OVER (
      PARTITION BY payment_type
      ORDER BY payment_value DESC
    ) AS payment_quartile

  FROM olist_order_payments
) AS ranked;

SELECT
  order_id,
  payment_type,
  payment_value,
  payment_installments,

  -- Previous and Next values
  LAG(payment_value) OVER (
    PARTITION BY payment_type
    ORDER BY payment_value
  ) AS previous_payment_value,

  LEAD(payment_value) OVER (
    PARTITION BY payment_type
    ORDER BY payment_value
  ) AS next_payment_value,

  -- First and Last values in the partition
  FIRST_VALUE(payment_value) OVER (
    PARTITION BY payment_type
    ORDER BY payment_value
  ) AS first_value_in_type,

  LAST_VALUE(payment_value) OVER (
    PARTITION BY payment_type
    ORDER BY payment_value
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS last_value_in_type,

  -- Cumulative and aggregate stats
  SUM(payment_value) OVER (
    PARTITION BY payment_type
    ORDER BY payment_value
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS running_total,

  AVG(payment_value) OVER (
    PARTITION BY payment_type
    ORDER BY payment_value
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
  ) AS moving_avg_3,

  MIN(payment_value) OVER (
    PARTITION BY payment_type
  ) AS min_payment_value,

  MAX(payment_value) OVER (
    PARTITION BY payment_type
  ) AS max_payment_value,

  COUNT(*) OVER (
    PARTITION BY payment_type
  ) AS count_per_type

FROM olist_order_payments
ORDER BY payment_type, payment_value;

## Top 5 customers & their product type purchase in a given period
SELECT
    c.customer_id,
    c.customer_unique_id,
    p.product_category_name,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(oi.price) AS total_spent
FROM olist_orders o
JOIN olist_customers c ON o.customer_id = c.customer_id
JOIN olist_order_items oi ON o.order_id = oi.order_id
JOIN olist_products p ON oi.product_id = p.product_id
WHERE o.order_purchase_timestamp BETWEEN '2017-01-01' AND '2018-01-01'
GROUP BY c.customer_id, c.customer_unique_id, p.product_category_name
ORDER BY total_spent DESC
LIMIT 5;

##⁠ Top 5 orders & order details
SELECT
    o.order_id,
    o.customer_id,
    o.order_status,
    o.order_purchase_timestamp,
    COUNT(oi.order_item_id) AS total_items,
    ROUND(SUM(oi.price), 2) AS total_order_price,
    ROUND(SUM(oi.freight_value), 2) AS total_shipping,
    ROUND(SUM(op.payment_value), 2) AS total_payment
FROM olist_orders o
JOIN olist_order_items oi ON o.order_id = oi.order_id
JOIN olist_order_payments op ON o.order_id = op.order_id
GROUP BY
    o.order_id,
    o.customer_id,
    o.order_status,
    o.order_purchase_timestamp
ORDER BY total_payment DESC
LIMIT 5;



## Top customer
SELECT
  c.customer_id,
  SUM(oi.price) AS total_spent
FROM olist_orders o
JOIN olist_customers c ON o.customer_id = c.customer_id
JOIN olist_order_items oi ON o.order_id = oi.order_id
GROUP BY c.customer_id
ORDER BY total_spent DESC
LIMIT 1;

