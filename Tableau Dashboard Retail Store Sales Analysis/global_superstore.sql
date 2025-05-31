-- Data Exploration: To get familiar with the Global Superstore dataset, I queried  global_superstore_raw to better understand the values stored in the table columns.

-- Product Categories: There were three distinct values in the category column: furniture, office supplies, and technology. I found that the furniture category contained four subcategories: bookcases, chairs, furnishings, and tables. Office supplies contained eight subcategories: appliances, art, binders, envelopes, fasteners, labels, paper, storage, and supplies. Finally, technology contained four subcategories: accessories, copiers, machines, and phones.

SELECT DISTINCT category
FROM global-superstore-450022.global_superstore.global_superstore_raw
ORDER BY category;

SELECT DISTINCT category, subcategory
FROM global-superstore-450022.global_superstore.global_superstore_raw
ORDER BY category, subcategory;

-- Customer Segments: There were three distinct values in the segment column: consumer, corporate, and home office.

SELECT DISTINCT segment
FROM global-superstore-450022.global_superstore.global_superstore_raw
ORDER BY segment;

-- Geographical Data: I wanted to understand the differences between the market, market2, and region columns. I discovered that the market and market2 columns contained identical values except for orders placed in Canada and the US. The market column contains two distinct values for these countries while the market2 column includes Canada and the US in North America. The values in the region column were much more granular, however the EMEA market/region was stored in the market, market2, and region columns.

SELECT DISTINCT market, market2, region
FROM global-superstore-450022.global_superstore.global_superstore_raw
ORDER BY market, market2, region;

SELECT COUNT (DISTINCT country) AS country_count
FROM global-superstore-450022.global_superstore.global_superstore_raw;

SELECT COUNT (DISTINCT state) AS state_count
FROM global-superstore-450022.global_superstore.global_superstore_raw;

-- Order Data: I found that the dataset consists of 25,035 orders placed from 2011 to 2014. A total of 10,292 distinct products were sold in these orders.

SELECT COUNT (DISTINCT order_id) AS order_count
FROM global-superstore-450022.global_superstore.global_superstore_raw;

SELECT COUNT (DISTINCT product_id) AS order_count
FROM global-superstore-450022.global_superstore.global_superstore_raw;

SELECT DISTINCT year
FROM global-superstore-450022.global_superstore.global_superstore_raw;

-- Aggregating orders by month and year: Now that I had a better understanding of the data, I started to aggregate the order data by year. I found that order volume steadily increased year over year. The total number of orders by year were as follows: 2011 (8,998), 2012 (10,962), 2013 (13,799), 2014 (17,531). In the final query listed in this section, I aggregated orders by month and year. After executing this query, I found that the months stored in the order_date column were numeric rather than string values (i.e. January, February, etc.) In order to show changes in order and sales volume from month to month, these values needed to be transformed into string values.

SELECT year, COUNT(*)
FROM global-superstore-450022.global_superstore.global_superstore_raw
GROUP BY year;

SELECT EXTRACT(MONTH FROM order_date) AS month, year, COUNT(*) AS number_of_orders
FROM global-superstore-450022.global_superstore.global_superstore_raw
GROUP BY EXTRACT(MONTH FROM order_date), year
ORDER BY month, year;

-- Aggregating sales and profit by year

SELECT year, SUM(sales) AS total_sales
FROM global-superstore-450022.global_superstore.global_superstore_raw
GROUP BY year;

SELECT year, ROUND(SUM(profit),2) AS total_profit
FROM global-superstore-450022.global_superstore.global_superstore_raw
GROUP BY year;

-- Creating staging table

CREATE TABLE global-superstore-450022.global_superstore.global_superstore_staging AS (
  SELECT
    order_id,
    order_date,
    year,
    category,
    subcategory,
    segment,
    country,
    market2 AS market,
    sales,
    profit - sales AS cost,
    profit
  FROM
    global-superstore-450022.global_superstore.global_superstore_raw
);

-- Adding column to staging table:

ALTER TABLE
  global-superstore-450022.global_superstore.global_superstore_staging
ADD COLUMN
  month STRING;

ALTER TABLE 
  global-superstore-450022.global_superstore.global_superstore_staging
ADD COLUMN
  quarter STRING;

-- Data Transformation

UPDATE
  global-superstore-450022.global_superstore.global_superstore_staging
SET
  month = CAST(EXTRACT(MONTH FROM order_date) AS STRING)
WHERE order_date IS NOT NULL;

UPDATE
  global-superstore-450022.global_superstore.global_superstore_staging
SET
  month = 'January'
WHERE
  month = '1';

UPDATE
  global-superstore-450022.global_superstore.global_superstore_staging
SET
  month = 'February'
WHERE
  month = '2';

UPDATE
  global-superstore-450022.global_superstore.global_superstore_staging
SET
  month = 'March'
WHERE
  month = '3';

UPDATE
  global-superstore-450022.global_superstore.global_superstore_staging
SET
  month = 'April'
WHERE
  month = '4';

UPDATE
  global-superstore-450022.global_superstore.global_superstore_staging
SET
  month = 'May'
WHERE
  month = '5';

UPDATE
  global-superstore-450022.global_superstore.global_superstore_staging
SET
  month = 'June'
WHERE
  month = '6';

UPDATE
  global-superstore-450022.global_superstore.global_superstore_staging
SET
  month = 'July'
WHERE
  month = '7';

UPDATE
  global-superstore-450022.global_superstore.global_superstore_staging
SET
  month = 'August'
WHERE
  month = '8';

UPDATE
  global-superstore-450022.global_superstore.global_superstore_staging
SET
  month = 'September'
WHERE
  month = '9';

UPDATE
  global-superstore-450022.global_superstore.global_superstore_staging
SET
  month = 'October'
WHERE
  month = '10';

UPDATE
  global-superstore-450022.global_superstore.global_superstore_staging
SET
  month = 'November'
WHERE
  month = '11';

UPDATE
  global-superstore-450022.global_superstore.global_superstore_staging
SET
  month = 'December'
WHERE
  month = '12';

-- Data Transformation

UPDATE
  global-superstore-450022.global_superstore.global_superstore_staging
SET 
  quarter = 'Q1'
WHERE 
  month IN ('January', 'February', 'March');

UPDATE
  global-superstore-450022.global_superstore.global_superstore_staging
SET 
  quarter = 'Q2'
WHERE 
  month IN ('April', 'May', 'June');

UPDATE
  global-superstore-450022.global_superstore.global_superstore_staging
SET 
  quarter = 'Q3'
WHERE 
  month IN ('July', 'August', 'September');

UPDATE
  global-superstore-450022.global_superstore.global_superstore_staging
SET 
  quarter = 'Q4'
WHERE 
  month IN ('October', 'November', 'December');

-- Creating final table

CREATE TABLE global-superstore-450022.global_superstore.global_superstore_final AS (
  SELECT
    order_id,
    order_date,
    month,
    quarter,
    year,
    category,
    subcategory,
    segment,
    country,
    market,
    sales,
    cost,
    profit
  FROM
    global-superstore-450022.global_superstore.global_superstore_staging
);

SELECT *
FROM global-superstore-450022.global_superstore.global_superstore_final;

-- Creating sub-tables

CREATE TABLE global-superstore-450022.global_superstore.kpis_by_year AS (
  SELECT 
       year, 
       SUM(sales) AS total_sales, 
       SUM(profit) AS total_profit,
       SUM(profit) / SUM(sales) AS profit_margin,
       COUNT(DISTINCT customer_id) AS num_of_customers,
       SUM(sales) / COUNT(DISTINCT order_id) AS avg_ticket_size,
       SUM(sales) / COUNT(DISTINCT customer_id) AS avg_customer_spend
  FROM `global-superstore-450022.global_superstore.global_superstore_raw`
  GROUP BY year
);

CREATE TABLE global-superstore-450022.global_superstore.global_superstore_yoy_growth AS (
  SELECT
    cy_py_sales_profit.year,
    cy_py_sales_profit.total_sales,
    cy_py_sales_profit.py_sales,
    (cy_py_sales_profit.total_sales - cy_py_sales_profit.py_sales) / cy_py_sales_profit.py_sales AS yoy_sales_growth,
    cy_py_sales_profit.total_profit,
    cy_py_sales_profit.py_profit,
    (cy_py_sales_profit.total_profit - cy_py_sales_profit.py_profit) / cy_py_sales_profit.py_profit AS yoy_profit_growth
  FROM (
    SELECT
      year, 
      total_sales,
      LAG(total_sales) OVER(ORDER BY year) AS py_sales,
      total_profit,
      LAG(total_profit) OVER(ORDER BY year) AS py_profit
    FROM `global-superstore-450022.global_superstore.kpis_by_year`
    ORDER BY year
  ) AS cy_py_sales_profit
);

CREATE TABLE `global-superstore-450022.global_superstore.global_superstore_customer_metrics` AS (

  WITH customer_years AS (
      SELECT 
          customer_id,
          year,
          order_id,
          SUM(sales) AS total_sales,
          SUM(profit) AS total_profit,
          MIN(order_date) AS first_order_date
      FROM `global-superstore-450022.global_superstore.global_superstore_raw` 
      GROUP BY customer_id, year, order_id
  ),
  customer_history AS (
      SELECT 
          customer_id,
          year,
          COUNT(DISTINCT order_id) AS orders_per_customer,  -- Count unique orders per customer
          SUM(total_sales) AS total_sales,
          SUM(total_profit) AS total_profit,
          MAX(year) OVER (PARTITION BY customer_id ORDER BY year ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS max_previous_year
      FROM customer_years
      GROUP BY customer_id, year
  )
  SELECT 
      year,
      CASE 
          WHEN max_previous_year IS NULL THEN 'New' 
          ELSE 'Existing' 
      END AS customer_type,
      COUNT(DISTINCT customer_id) AS num_customers,
      SUM(orders_per_customer) AS num_orders,  -- Aggregate orders per customer instead of counting distinct orders
      SUM(total_sales) AS total_sales,
      SUM(total_profit) AS total_profit,

      SUM(total_sales) / SUM(orders_per_customer) AS average_ticket_size,
      SUM(total_sales) / COUNT(DISTINCT customer_id) AS avg_total_spend
  FROM customer_history
  WHERE year > 2011
  GROUP BY year, customer_type
  ORDER BY year, customer_type
);