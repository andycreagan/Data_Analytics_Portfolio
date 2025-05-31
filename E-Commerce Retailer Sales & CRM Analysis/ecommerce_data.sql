-- Create staging table and alter data types for invoice_date and customer_id columns
CREATE OR REPLACE TABLE `marketing-analytics-452523.ecommerce_data.ecommerce_data_stage` AS
  (SELECT
    invoice_num,
    stock_code,
    description,
    quantity,
    PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', CONCAT(invoice_date, ':00')) AS invoice_date,
    unit_price,
    CAST(customer_id AS STRING) AS customer_id,
    country
  FROM `marketing-analytics-452523.ecommerce_data.ecommerce_data_raw`);

  -- Checking for duplicate values in ecommerce_data_stage

  -- Counting total number of rows in ecommerce_data_stage
  SELECT COUNT(*) AS total_rows FROM `marketing-analytics-452523.ecommerce_data.ecommerce_data_stage`;

  -- Counting total number of distinct rows (non-duplicate rows) in ecommerce_data_stage
  SELECT COUNT(*) AS distinct_rows
  FROM 
    (SELECT DISTINCT
      invoice_num,
      stock_code,
      description,
      quantity,
      invoice_date,
      unit_price,
      customer_id,
      country
    FROM `marketing-analytics-452523.ecommerce_data.ecommerce_data_stage`);

    -- Calculating duplicate_rows by subtracting distinct_rows from total_rows via num_rows CTE
    WITH num_rows AS
      (SELECT
        (SELECT COUNT(*) 
        FROM `marketing-analytics-452523.ecommerce_data.ecommerce_data_stage`) AS total_rows,
        (SELECT COUNT(*) FROM 
          (SELECT DISTINCT
            invoice_num,
            stock_code,
            description,
            quantity,
            invoice_date,
            unit_price,
            customer_id,
            country
           FROM `marketing-analytics-452523.ecommerce_data.ecommerce_data_stage`)) AS distinct_rows)

SELECT
  total_rows,
  distinct_rows,
  total_rows - distinct_rows AS duplicate_rows
FROM num_rows;

-- Create final table that removes duplicates and includes sales_amount, invoice_year, invoice_month
CREATE OR REPLACE TABLE `marketing-analytics-452523.ecommerce_data.ecommerce_data_final` AS
SELECT DISTINCT
  invoice_num,
  stock_code,
  description,
  quantity,
  invoice_date,
  unit_price,
  customer_id,
  country,
  EXTRACT(YEAR FROM invoice_date) AS invoice_year,
  EXTRACT(MONTH FROM invoice_date) AS invoice_month,
  FORMAT_TIMESTAMP('%Y-%m', invoice_date) AS year_month,
  quantity * unit_price AS sales_amount
FROM `marketing-analytics-452523.ecommerce_data.ecommerce_data_stage`
WHERE quantity > 0 AND unit_price > 0;

-- Create final cleaned table that removes outlier in quantity column
CREATE OR REPLACE TABLE `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned` AS
SELECT *
FROM `marketing-analytics-452523.ecommerce_data.ecommerce_data_final`
WHERE quantity < 80995;

-- Create v2 of final cleaned table that removes outlier in quantity column
CREATE OR REPLACE TABLE `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v2` AS
SELECT *
FROM `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned`
WHERE quantity <= 4800;

-- Identify stock_codes with high unit_prices
SELECT   invoice_num, customer_id, stock_code, description, unit_price
FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v2` 
ORDER BY unit_price DESC;

-- Count the number of rows containing postage, manual, or miscellaneous stock codes
SELECT COUNT(*)
FROM   `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v2` 
WHERE  stock_code IN ('AMAZONFEE', 'B', 'POST', 'DOT', 'M');

-- Calculate percentage of rows containing postage, manual, or miscellaneous stock codes
SELECT 
  ROUND(
    (SELECT COUNT(*) * 1.0
     FROM   `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v2` 
     WHERE  stock_code IN ('AMAZONFEE', 'B', 'POST', 'DOT', 'M')) / 
    (SELECT COUNT(*) 
     FROM   `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v2`) * 100, 1)
     AS pct_misc;

-- Identify any other miscellaneous stock_codes in the data
SELECT    DISTINCT stock_code, description, 
          COUNT(*) AS num_rows,
          MAX(unit_price) AS max_price
FROM      `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v2`
GROUP BY  stock_code, description
ORDER BY  max_price DESC
LIMIT 20;

-- Create v3 of final cleaned table that removes outliers in unit_price column
CREATE OR REPLACE TABLE `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3` AS

  (SELECT *
  FROM `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v2`
  WHERE stock_code NOT IN ('AMAZONFEE', 'B', 'POST', 'DOT', 'M'));

-- Calculate sales by month
SELECT year_month, SUM(sales_amount) AS monthly_sales
FROM `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
GROUP BY year_month
ORDER BY year_month;

-- Show prior month sales by using LAG()
WITH ms AS
  (SELECT year_month, SUM(sales_amount) AS monthly_sales
  FROM `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
  GROUP BY year_month
  ORDER BY year_month)

SELECT year_month, monthly_sales,
       LAG(monthly_sales) OVER(ORDER BY year_month) AS pm_sales
FROM ms
ORDER BY year_month;

-- Calculate % change month over month
WITH ms AS
  (SELECT year_month, SUM(sales_amount) AS monthly_sales
  FROM `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
  GROUP BY year_month
  ORDER BY year_month),

     pm AS
  (SELECT year_month, monthly_sales,
   LAG(monthly_sales) OVER(ORDER BY year_month) AS pm_sales
   FROM ms
   ORDER BY year_month)

SELECT year_month, monthly_sales, pm_sales,
       ROUND(((monthly_sales - pm_sales) / pm_sales) * 100, 2) AS pct_change
FROM pm;

-- Monthly sales in the United Kingdom
SELECT   year_month, 
         SUM(sales_amount) AS uk_monthly_sales
FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
WHERE    country = 'United Kingdom'
GROUP BY year_month
ORDER BY year_month;

-- Calculate percentage of monthly UK sales in monthly total sales
WITH uk_monthly_sales AS

  (SELECT  year_month, 
           SUM(sales_amount) AS uk_monthly_sales
  FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
  WHERE    country = 'United Kingdom'
  GROUP BY year_month
  ORDER BY year_month),

      total_monthly_sales AS

  (SELECT  year_month, 
           SUM(sales_amount) AS monthly_sales
  FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
  GROUP BY year_month
  ORDER BY year_month)

SELECT     uk_monthly_sales.year_month,
           uk_monthly_sales.uk_monthly_sales,
           total_monthly_sales.monthly_sales,
      ROUND((uk_monthly_sales.uk_monthly_sales / 
           total_monthly_sales.monthly_sales) * 100.0, 1) AS pct_uk
FROM       uk_monthly_sales
INNER JOIN total_monthly_sales
    ON     uk_monthly_sales.year_month = total_monthly_sales.year_month
ORDER BY   year_month;

-- Count number of transactions by month
SELECT year_month, COUNT(DISTINCT invoice_num) AS monthly_transactions
FROM `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
GROUP BY year_month
ORDER BY year_month;

-- Show prior month transactions by using LAG()
WITH mt AS
  (SELECT year_month, COUNT(DISTINCT invoice_num) AS monthly_transactions
   FROM `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
   GROUP BY year_month
   ORDER BY year_month)

SELECT year_month, monthly_transactions,
       LAG(monthly_transactions) OVER(ORDER BY year_month) AS pm_transactions
FROM mt
ORDER BY year_month;

-- Calculate % change in transactions month over month
WITH mt AS
  (SELECT year_month, COUNT(DISTINCT invoice_num) AS monthly_transactions
   FROM `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
   GROUP BY year_month
   ORDER BY year_month),

     pt AS
  (SELECT year_month, monthly_transactions,
          LAG(monthly_transactions) OVER(ORDER BY year_month) AS pm_transactions
   FROM mt
   ORDER BY year_month)

SELECT year_month, monthly_transactions, pm_transactions,
       ROUND(((monthly_transactions - pm_transactions) / pm_transactions) * 100, 2) AS pct_change
FROM pt;

-- Calculate % change in sales & transactions month over month
WITH 

  ms AS

  (SELECT year_month, SUM(sales_amount) AS monthly_sales
   FROM `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
   GROUP BY year_month
   ORDER BY year_month),

  pm AS

  (SELECT year_month, monthly_sales,
   LAG(monthly_sales) OVER(ORDER BY year_month) AS pm_sales
   FROM ms
   ORDER BY year_month),

  mt AS

  (SELECT year_month, COUNT(DISTINCT invoice_num) AS monthly_transactions
   FROM `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
   GROUP BY year_month
   ORDER BY year_month),

  pt AS
  (SELECT year_month, monthly_transactions,
          LAG(monthly_transactions) OVER(ORDER BY year_month) AS pm_transactions
   FROM mt
   ORDER BY year_month)

SELECT pm.year_month, 
       pm.monthly_sales,
       ((pm.monthly_sales - pm.pm_sales) / pm.pm_sales) AS pct_change_sales,
       pt.monthly_transactions,
       ((pt.monthly_transactions - pt.pm_transactions) / pt.pm_transactions) AS pct_change_transactions
FROM pm
JOIN pt
  ON pm.year_month = pt.year_month;

-- View 10 products by sales
SELECT   stock_code, 
         description,
         SUM(sales_amount) AS total_sales
FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
GROUP BY stock_code, description
ORDER BY total_sales DESC
LIMIT 10;

-- View 10 most ordered products
SELECT   stock_code, 
         description,
         SUM(quantity) AS total_quantity_sold
FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
GROUP BY stock_code, description
ORDER BY total_quantity_sold DESC
LIMIT 10;

-- View total sales and total quantity sold for each product
SELECT   stock_code, 
         INITCAP(description) AS description,
         SUM(sales_amount) AS total_sales,
         SUM(quantity) AS total_quantity_sold
FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
GROUP BY stock_code, description;

-- Normalize descriptions for stock codes 22197, 22502, and 85123A
SELECT stock_code, 
       CASE 
          WHEN stock_code = '22197' THEN 'Popcorn Holder'
          WHEN stock_code = '22502' THEN 'Picnic Basket Wicker'
          WHEN stock_code = '85123A' THEN 'Hanging Heart T-Light Holder'
          ELSE INITCAP(description)
        END AS description,
        SUM(sales_amount) AS total_sales,
        SUM(quantity) AS total_quantity_sold
FROM `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
GROUP BY stock_code, description;

-- View top 10 stock code transactions by month
SELECT   year_month, COUNT(*) AS times_ordered
FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
WHERE    stock_code IN ('85123A', '85099B', '22423', '47566', '20725', '84879', '22197', '22720', 
                        '21212', '22383')
GROUP BY year_month
ORDER BY year_month;

-- View top 10 countries by number of invoices/transactions
SELECT   country, COUNT(DISTINCT invoice_num) AS num_invoices
FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
GROUP BY country
ORDER BY COUNT(DISTINCT invoice_num) DESC
LIMIT 10;

-- View UK invoices/transactions by month
SELECT   year_month, COUNT(DISTINCT invoice_num) AS num_invoices
FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
WHERE    country = 'United Kingdom'
GROUP BY year_month
ORDER BY year_month;

-- Calculate percentage of monthly UK transactions in monthly total transactions
WITH uk_monthly_transactions AS

  (SELECT  year_month, 
           COUNT(DISTINCT invoice_num) AS uk_monthly_transactions
  FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
  WHERE    country = 'United Kingdom'
  GROUP BY year_month
  ORDER BY year_month),

      total_monthly_transactions AS

  (SELECT  year_month, 
           COUNT(DISTINCT invoice_num) AS monthly_transactions
  FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
  GROUP BY year_month
  ORDER BY year_month)

SELECT     uk_monthly_transactions.year_month,
           uk_monthly_transactions.uk_monthly_transactions,
           total_monthly_transactions.monthly_transactions,
      ROUND((uk_monthly_transactions.uk_monthly_transactions / 
           total_monthly_transactions.monthly_transactions) * 100.0, 1) AS pct_uk
FROM       uk_monthly_transactions
INNER JOIN total_monthly_transactions
    ON     uk_monthly_transactions.year_month = total_monthly_transactions.year_month
ORDER BY   year_month;

-- Calculate percentage of UK sales & transactions in total monthly sales & transactions
WITH uk_monthly_sales AS

  (SELECT  year_month, 
           SUM(sales_amount) AS uk_monthly_sales
   FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
   WHERE    country = 'United Kingdom'
   GROUP BY year_month
   ORDER BY year_month),

      total_monthly_sales AS

  (SELECT  year_month, 
           SUM(sales_amount) AS monthly_sales
   FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
   GROUP BY year_month
   ORDER BY year_month),

       uk_monthly_transactions AS

  (SELECT  year_month, 
           COUNT(DISTINCT invoice_num) AS uk_monthly_transactions
   FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
   WHERE    country = 'United Kingdom'
   GROUP BY year_month
   ORDER BY year_month),

      total_monthly_transactions AS

  (SELECT  year_month, 
           COUNT(DISTINCT invoice_num) AS monthly_transactions
   FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
   GROUP BY year_month
   ORDER BY year_month)

SELECT     uk_monthly_sales.year_month,
           uk_monthly_sales.uk_monthly_sales,
           total_monthly_sales.monthly_sales,
           uk_monthly_sales.uk_monthly_sales / total_monthly_sales.monthly_sales AS pct_sales_uk,
           uk_monthly_transactions.uk_monthly_transactions,
           total_monthly_transactions.monthly_transactions,
           uk_monthly_transactions.uk_monthly_transactions / 
           total_monthly_transactions.monthly_transactions AS pct_transations_uk
FROM       uk_monthly_sales
JOIN       total_monthly_sales
    ON     uk_monthly_sales.year_month = total_monthly_sales.year_month
JOIN       uk_monthly_transactions
    ON     uk_monthly_transactions.year_month = total_monthly_sales.year_month
JOIN       total_monthly_transactions
    ON     uk_monthly_transactions.year_month = total_monthly_transactions.year_month;

-- Calculate average sales per transaction for Australia, France, Germany, Ireland, and Netherlands
SELECT   country,
         SUM(sales_amount) AS total_sales,
         COUNT(DISTINCT invoice_num) AS num_transactions,
         SUM(sales_amount) / COUNT(DISTINCT invoice_num) AS sales_per_transaction
FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
WHERE    country IN ('Australia', 'EIRE', 'France', 'Germany', 'Netherlands')
GROUP BY country;

-- Total sales
SELECT SUM(sales_amount) AS total_sales
FROM `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`;

-- Total number of transactions
SELECT COUNT (DISTINCT invoice_num) AS total_transactions
FROM `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`;

-- Average sales per transaction
SELECT
  ROUND(
    (SELECT SUM(sales_amount) 
    FROM `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`) / 
    (SELECT COUNT (DISTINCT invoice_num) 
    FROM `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`), 2) 
    AS avg_sales_transaction;

-- Calculate average quantity sold by stock_code
WITH tqs AS

  (SELECT   stock_code, SUM(quantity) AS total_quantity_sold
   FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
   GROUP BY stock_code),

     tt AS
  (SELECT   stock_code, COUNT(DISTINCT invoice_num) AS num_invoices
   FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
   GROUP BY stock_code)

SELECT   tqs.stock_code,
         ROUND(
            tqs.total_quantity_sold / tt.num_invoices, 0) AS avg_quantity_sold
FROM     tqs
JOIN     tt
  ON     tqs.stock_code = tt.stock_code
ORDER BY avg_quantity_sold DESC
LIMIT 20;

-- Investigate stock_code 47556B
SELECT *
FROM   `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
WHERE  stock_code = '47556B';

-- Investigating invoices with stock_code 47556B
SELECT   *
FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
WHERE    invoice_num IN('550461', '540818')
ORDER BY invoice_num;

SELECT   invoice_num, SUM(sales_amount) AS total_sales
FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
WHERE    invoice_num IN('550461', '540818')
GROUP BY invoice_num
ORDER BY invoice_num;

-- Calculate average sales amount per transaction by stock_code
WITH ts AS

  (SELECT   stock_code, SUM(sales_amount) AS total_sales
   FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
   GROUP BY stock_code),

     tt AS
  (SELECT   stock_code, COUNT(DISTINCT invoice_num) AS num_invoices
   FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
   GROUP BY stock_code)

SELECT   ts.stock_code,
         ROUND(
            ts.total_sales / tt.num_invoices, 0) AS avg_sales_per_transaction
FROM     ts
JOIN     tt
  ON     ts.stock_code = tt.stock_code
ORDER BY avg_sales_per_transaction DESC
LIMIT 20;

-- Calculate average quantity sold per transaction per country
WITH tqs AS

  (SELECT   country, SUM(quantity) AS total_quantity_sold
   FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
   GROUP BY country),

     tt AS
  (SELECT   country, COUNT(DISTINCT invoice_num) AS num_transactions
   FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
   GROUP BY country)

SELECT   tqs.country,
         ROUND(
            tqs.total_quantity_sold / tt.num_transactions, 0) AS avg_quantity_sold
FROM     tqs
JOIN     tt
  ON     tqs.country = tt.country
ORDER BY avg_quantity_sold DESC;

-- View total unique transactions per country, total quantity sold per country, and average quantity sold per transaction per country
WITH tqs AS

  (SELECT   country, SUM(quantity) AS total_quantity_sold
   FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
   GROUP BY country),

     tt AS
  (SELECT   country, COUNT(DISTINCT invoice_num) AS num_transactions
   FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
   GROUP BY country)

SELECT   tqs.country,
         tt.num_transactions,
         tqs.total_quantity_sold,
         ROUND(
            tqs.total_quantity_sold / tt.num_transactions, 0) AS avg_quantity_sold
FROM     tqs
JOIN     tt
  ON     tqs.country = tt.country
ORDER BY avg_quantity_sold DESC;

-- Calculate average sales per transaction per country
WITH ts AS

  (SELECT   country, SUM(sales_amount) AS total_sales
   FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
   GROUP BY country),

     tt AS
  (SELECT   country, COUNT(DISTINCT invoice_num) AS num_transactions
   FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
   GROUP BY country)

SELECT   ts.country,
         ROUND(
            ts.total_sales / tt.num_transactions, 0) AS avg_sales
FROM     ts
JOIN     tt
  ON     ts.country = tt.country
ORDER BY avg_sales DESC;

-- View total unique transactions per country, total sales per country, and total sales per transaction per country
WITH ts AS

  (SELECT   country, SUM(sales_amount) AS total_sales
   FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
   GROUP BY country),

     tt AS
  (SELECT   country, COUNT(DISTINCT invoice_num) AS num_transactions
   FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
   GROUP BY country)

SELECT   ts.country,
         tt.num_transactions,
         ts.total_sales,
         ROUND(
            ts.total_sales / tt.num_transactions, 0) AS avg_sales
FROM     ts
JOIN     tt
  ON     ts.country = tt.country
ORDER BY avg_sales DESC;

-- Calculate average quantity sold per transaction per customer_id
WITH tqs AS

  (SELECT   customer_id, SUM(quantity) AS total_quantity_sold
   FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
   GROUP BY customer_id),

     tt AS
  (SELECT   customer_id, COUNT(DISTINCT invoice_num) AS num_transactions
   FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
   GROUP BY customer_id)

SELECT   tqs.customer_id,
         ROUND(
            tqs.total_quantity_sold / tt.num_transactions, 0) AS avg_quantity_sold
FROM     tqs
JOIN     tt
  ON     tqs.customer_id = tt.customer_id
ORDER BY avg_quantity_sold DESC;

-- Show number of transactions and average quantity sold per transaction per customer_id
WITH tqs AS

  (SELECT   customer_id, SUM(quantity) AS total_quantity_sold
   FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
   GROUP BY customer_id),

     tt AS
  (SELECT   customer_id, COUNT(DISTINCT invoice_num) AS num_transactions
   FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
   GROUP BY customer_id)

SELECT   tqs.customer_id,
         tt.num_transactions,
         ROUND(
            tqs.total_quantity_sold / tt.num_transactions, 0) AS avg_quantity_sold
FROM     tqs
JOIN     tt
  ON     tqs.customer_id = tt.customer_id
ORDER BY avg_quantity_sold DESC
LIMIT 20;

-- Calculate average number of transactions per customer
SELECT
  (SELECT COUNT(DISTINCT invoice_num)
   FROM   `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`) * 1.0 /
  (SELECT COUNT(DISTINCT customer_id)
   FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`)
   AS avg_transactions_per_customer;

-- Filter to include customers with 5+ unique orders
WITH tqs AS

  (SELECT   customer_id, SUM(quantity) AS total_quantity_sold
   FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
   GROUP BY customer_id),

     tt AS
  (SELECT   customer_id, COUNT(DISTINCT invoice_num) AS num_transactions
   FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
   GROUP BY customer_id)

SELECT   tqs.customer_id,
         tt.num_transactions,
         ROUND(
            tqs.total_quantity_sold / tt.num_transactions, 0) AS avg_quantity_sold
FROM     tqs
JOIN     tt
  ON     tqs.customer_id = tt.customer_id
WHERE    tt.num_transactions >= 5
ORDER BY avg_quantity_sold DESC;

-- Calculate average sales per transaction per customer_id
WITH ts AS

  (SELECT   customer_id, SUM(sales_amount) AS total_sales
   FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
   GROUP BY customer_id),

     tt AS
  (SELECT   customer_id, COUNT(DISTINCT invoice_num) AS num_transactions
   FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
   GROUP BY customer_id)

SELECT   ts.customer_id,
         ROUND(
            ts.total_sales / tt.num_transactions, 0) AS avg_sales
FROM     ts
JOIN     tt
  ON     ts.customer_id = tt.customer_id
ORDER BY avg_sales DESC;

-- Show number of transactions, average items per transaction, and average sales per transaction per customer_id (minimum 5 transactions)
WITH tqs AS

  (SELECT   customer_id, SUM(quantity) AS total_quantity_sold
   FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
   GROUP BY customer_id),

     ts AS
    
    (SELECT   customer_id, SUM(sales_amount) AS total_sales
     FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
     GROUP BY customer_id),

     tt AS
  (SELECT   customer_id, COUNT(DISTINCT invoice_num) AS num_transactions
   FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
   GROUP BY customer_id)

SELECT   tqs.customer_id,
         tt.num_transactions,
         ROUND(
            tqs.total_quantity_sold / tt.num_transactions, 0) AS avg_items_per_transaction,
         ROUND(
            ts.total_sales / tt.num_transactions, 0) AS avg_sales_per_transaction
FROM     tqs
JOIN     tt
  ON     tqs.customer_id = tt.customer_id
JOIN     ts
  ON     tt.customer_id = ts.customer_id
WHERE    tt.num_transactions >= 5
ORDER BY avg_sales_per_transaction DESC;

-- Group transactions by customer and month
SELECT   customer_id, year_month,
         COUNT(DISTINCT invoice_num) AS total_transactions
FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
GROUP BY customer_id, year_month;

-- Calculate average transactions by customer and month
WITH tt AS

  (SELECT   customer_id, year_month,
            COUNT(DISTINCT invoice_num) AS total_transactions
  FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
  GROUP BY customer_id, year_month)

SELECT ROUND(AVG(total_transactions),2) AS avg_transactions
FROM   tt;

-- Calculate average transactions per month per customer (minimum 5 transactions)
WITH customer_trans AS

  (SELECT   customer_id,
            COUNT(DISTINCT invoice_num) AS total_transactions
  FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
  GROUP BY  customer_id),

     num_months AS

  (SELECT COUNT(DISTINCT year_month) AS num_months
  FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`)

SELECT     ct.customer_id,
           ct.total_transactions,
           (ct.total_transactions * 1.0) / nm.num_months AS avg_transactions_per_month 
FROM       customer_trans ct
CROSS JOIN num_months nm
WHERE      ct.customer_id IS NOT NULL
AND        ct.total_transactions >= 5
ORDER BY   avg_transactions_per_month DESC;

-- Show % of total sales, sales, number of transactions, average items per transaction, average sales per transaction, and average transactions per month by customer_id (minimum 5 transactions)
WITH totals AS

  (SELECT   customer_id, 
            COUNT(DISTINCT invoice_num) AS num_transactions,
            SUM(quantity) AS total_quantity,
            SUM(sales_amount) AS total_sales
   FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
   GROUP BY customer_id),
  
      months AS
  (SELECT COUNT(DISTINCT year_month) AS num_months
  FROM `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`)

SELECT   t.customer_id,
         (total_sales / (SELECT SUM(sales_amount) 
         FROM `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`)) 
         AS pct_sales,
         total_sales,
         t.num_transactions,
         ROUND(
            t.total_sales / t.num_transactions, 0) AS avg_sales_per_transaction,
         ROUND(
            t.total_quantity / t.num_transactions, 0) AS avg_items_per_transaction,
         ROUND(
            t.num_transactions / m.num_months, 0) AS avg_transactions_per_month
FROM       totals t
CROSS JOIN months m
WHERE      num_transactions >= 5
ORDER BY   avg_sales_per_transaction DESC;

-- Average sales per transaction, average items per transaction, average transactions per month per customer (minimum 5 transactions)
WITH mt AS

  (SELECT  customer_id, 
           year_month,
           COUNT(DISTINCT invoice_num) AS monthly_transactions
  FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
  GROUP BY customer_id, year_month),

     customer_totals AS

  (SELECT   customer_id,
            COUNT(DISTINCT invoice_num) AS total_transactions,
            SUM(quantity) AS total_quantity,
            SUM(sales_amount) AS total_sales,
            COUNT(DISTINCT year_month) AS num_months
   FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
   GROUP BY customer_id),

     filtered_customers AS

  (SELECT *
   FROM   customer_totals
   WHERE  total_transactions >= 5)

SELECT
  SUM(total_sales) / SUM(total_transactions) AS avg_sales_per_transaction,
  SUM(total_quantity) / SUM(total_transactions) AS avg_items_per_transaction,
  SUM(total_transactions) / SUM(num_months) AS avg_transactions_per_month_per_customer
FROM filtered_customers;

-- Number of transactions, average items per transaction, average sales per transaction, and average transactions per month by customer_id (minimum 2 transactions/repeat customers)
WITH totals AS

  (SELECT   customer_id, 
            COUNT(DISTINCT invoice_num) AS num_transactions,
            SUM(quantity) AS total_quantity,
            SUM(sales_amount) AS total_sales
   FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
   GROUP BY customer_id),
  
      months AS
  (SELECT COUNT(DISTINCT year_month) AS num_months
  FROM `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`)

SELECT   t.customer_id,
         t.num_transactions,
         ROUND(
            t.total_sales / t.num_transactions, 0) AS avg_sales_per_transaction,
         ROUND(
            t.total_quantity / t.num_transactions, 0) AS avg_items_per_transaction,
         ROUND(
            t.num_transactions / m.num_months, 0) AS avg_transactions_per_month
FROM       totals t
CROSS JOIN months m
WHERE      num_transactions > 1
ORDER BY   avg_sales_per_transaction DESC;

-- Average sales per transaction, average items per transaction, average transactions per month per customer (minimum 2 transactions/repeat customers)
WITH mt AS

  (SELECT  customer_id, 
           year_month,
           COUNT(DISTINCT invoice_num) AS monthly_transactions
  FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
  GROUP BY customer_id, year_month),

     customer_totals AS

  (SELECT   customer_id,
            COUNT(DISTINCT invoice_num) AS total_transactions,
            SUM(quantity) AS total_quantity,
            SUM(sales_amount) AS total_sales,
            COUNT(DISTINCT year_month) AS num_months
   FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
   GROUP BY customer_id),

     filtered_customers AS

  (SELECT *
   FROM   customer_totals
   WHERE  total_transactions > 1)

SELECT
  SUM(total_sales) / SUM(total_transactions) AS avg_sales_per_transaction,
  SUM(total_quantity) / SUM(total_transactions) AS avg_items_per_transaction,
  SUM(total_transactions) / SUM(num_months) AS avg_transactions_per_month_per_customer
FROM filtered_customers;

-- Number of transactions, average items per transaction, average sales per transaction, and average transactions per month by customer_id (1 transaction/one-time customers)
WITH totals AS

  (SELECT   customer_id, 
            COUNT(DISTINCT invoice_num) AS num_transactions,
            SUM(quantity) AS total_quantity,
            SUM(sales_amount) AS total_sales
   FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
   GROUP BY customer_id),
  
      months AS
  (SELECT COUNT(DISTINCT year_month) AS num_months
  FROM `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`)

SELECT   t.customer_id,
         t.num_transactions,
         ROUND(
            t.total_sales / t.num_transactions, 0) AS avg_sales_per_transaction,
         ROUND(
            t.total_quantity / t.num_transactions, 0) AS avg_items_per_transaction,
         ROUND(
            t.num_transactions / m.num_months, 0) AS avg_transactions_per_month
FROM       totals t
CROSS JOIN months m
WHERE      num_transactions = 1
ORDER BY   avg_sales_per_transaction DESC;

-- Average sales per transaction, average items per transaction, average transactions per month per customer (1 transaction/one-time customers)
WITH mt AS

  (SELECT  customer_id, 
           year_month,
           COUNT(DISTINCT invoice_num) AS monthly_transactions
  FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
  GROUP BY customer_id, year_month),

     customer_totals AS

  (SELECT   customer_id,
            COUNT(DISTINCT invoice_num) AS total_transactions,
            SUM(quantity) AS total_quantity,
            SUM(sales_amount) AS total_sales,
            COUNT(DISTINCT year_month) AS num_months
   FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
   GROUP BY customer_id),

     filtered_customers AS

  (SELECT *
   FROM   customer_totals
   WHERE  total_transactions = 1)

SELECT
  SUM(total_sales) / SUM(total_transactions) AS avg_sales_per_transaction,
  SUM(total_quantity) / SUM(total_transactions) AS avg_items_per_transaction,
  SUM(total_transactions) / SUM(num_months) AS avg_transactions_per_month_per_customer
FROM filtered_customers;

-- Average sales per transaction, average items per transaction, average transactions per month per customer (overall)
WITH mt AS

  (SELECT  customer_id, 
           year_month,
           COUNT(DISTINCT invoice_num) AS monthly_transactions
  FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
  GROUP BY customer_id, year_month),

     customer_totals AS

  (SELECT   customer_id,
            COUNT(DISTINCT invoice_num) AS total_transactions,
            SUM(quantity) AS total_quantity,
            SUM(sales_amount) AS total_sales,
            COUNT(DISTINCT year_month) AS num_months
   FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
   GROUP BY customer_id),

     filtered_customers AS

  (SELECT *
   FROM   customer_totals)

SELECT
  SUM(total_sales) / SUM(total_transactions) AS avg_sales_per_transaction,
  SUM(total_quantity) / SUM(total_transactions) AS avg_items_per_transaction,
  SUM(total_transactions) / SUM(num_months) AS avg_transactions_per_month_per_customer
FROM filtered_customers;

-- Create summary table showing percentage distribution of one-time and repeat customers, plus number of customers, average sales per transaction, average items per transaction, and average transactions per month for one-time customers, repeat customers, and overall
WITH customer_totals AS (
    SELECT 
        customer_id,
        COUNT(DISTINCT invoice_num) AS total_transactions,
        SUM(quantity) AS total_quantity,
        SUM(sales_amount) AS total_sales,
        COUNT(DISTINCT year_month) AS num_months
    FROM 
        `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
    GROUP BY customer_id
),

customer_counts AS (
    SELECT COUNT(*) AS total_customers FROM customer_totals
),

overall AS (
    SELECT 
        'Overall' AS customer_type,
        NULL AS percent_of_customers,
        COUNT(*) AS num_customers,
        SUM(total_sales) / SUM(total_transactions) AS avg_sales_per_transaction,
        SUM(total_quantity) / SUM(total_transactions) AS avg_items_per_transaction,
        SUM(total_transactions) / SUM(num_months) AS avg_transactions_per_month
    FROM customer_totals
),

one_time AS (
    SELECT 
        'One-Time' AS customer_type,
        COUNT(*) / (SELECT total_customers FROM customer_counts) AS percent_of_customers,
        COUNT(*) AS num_customers,
        SUM(total_sales) / SUM(total_transactions) AS avg_sales_per_transaction,
        SUM(total_quantity) / SUM(total_transactions) AS avg_items_per_transaction,
        SUM(total_transactions) / SUM(num_months) AS avg_transactions_per_month
    FROM customer_totals
    WHERE total_transactions = 1
),

repeat AS (
    SELECT 
        'Repeat' AS customer_type,
        COUNT(*) / (SELECT total_customers FROM customer_counts) AS percent_of_customers,
        COUNT(*) AS num_customers,
        SUM(total_sales) / SUM(total_transactions) AS avg_sales_per_transaction,
        SUM(total_quantity) / SUM(total_transactions) AS avg_items_per_transaction,
        SUM(total_transactions) / SUM(num_months) AS avg_transactions_per_month
    FROM customer_totals
    WHERE total_transactions > 1
)

SELECT * FROM one_time
UNION ALL
SELECT * FROM repeat
UNION ALL
SELECT * FROM overall;

-- Average items per transaction
SELECT
  ROUND(
    (SELECT SUM(quantity) AS total_quantity_ordered
    FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`) / 
    (SELECT COUNT (DISTINCT invoice_num) AS total_transactions
    FROM `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`), 1) 
    AS avg_items_ordered;

-- Top selling products by unit sales
SELECT   stock_code, description, SUM(quantity) AS total_unit_sales
FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
GROUP BY stock_code, description
ORDER BY total_unit_sales DESC
LIMIT 10;

-- Top selling products by sales revenue
SELECT   stock_code, description, SUM(sales_amount) AS total_sales_revenue
FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
GROUP BY stock_code, description
ORDER BY total_sales_revenue DESC
LIMIT 10;

-- Calculating average quantity sold for products in top 10 sold by unit sales and revenue
WITH product_transaction_counts AS 
  
  (SELECT   stock_code,
            COUNT(DISTINCT invoice_num) AS num_transactions
   FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
   WHERE    stock_code IN ('23084', '23166', '85099B', '85123A')
   GROUP BY stock_code)

SELECT   f.stock_code,
         ROUND(SUM(f.quantity) / pt.num_transactions, 1) AS avg_quantity_per_transaction
FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3` f
JOIN     product_transaction_counts pt
  ON     f.stock_code = pt.stock_code
WHERE    f.stock_code IN ('23084', '23166', '85099B', '85123A')
GROUP BY f.stock_code, pt.num_transactions
ORDER BY f.stock_code;

-- Countries sorted by sales revenue
SELECT   country, SUM(sales_amount) AS total_sales
FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
GROUP BY country
ORDER BY total_sales DESC
LIMIT 5;

-- Creating logic for repeat customers
SELECT   customer_id, COUNT(DISTINCT invoice_num) AS num_orders
FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
GROUP BY customer_id
HAVING   num_orders > 1;

-- Counting number of repeat customers
WITH rc AS

  (SELECT   customer_id, COUNT(DISTINCT invoice_num) AS num_orders
  FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
  GROUP BY customer_id
  HAVING   num_orders > 1)

SELECT COUNT(*) AS repeat_customers
FROM rc;

-- Total number of customers
SELECT COUNT(DISTINCT customer_id) AS total_customers
FROM   `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`;

-- Calculate percentage of repeat customers
WITH rc AS

  (SELECT  customer_id, COUNT(DISTINCT invoice_num) AS num_orders
  FROM     `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`
  GROUP BY customer_id
  HAVING   num_orders > 1)

SELECT
  ROUND(
    (SELECT COUNT(*) FROM rc) /
    (SELECT COUNT(DISTINCT customer_id)
    FROM `marketing-analytics-452523.ecommerce_data.ecommerce_data_final_cleaned_v3`) * 100, 1) 
    AS pct_repeat;