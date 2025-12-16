CREATE DATABASE IF NOT EXISTS project_results;
USE project_results;

-- Create external table
CREATE EXTERNAL TABLE sales_data (
    product_id INT,
    product_name STRING,
    quantity INT,
    price FLOAT,
    sale_date STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/tmp/datasets/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Show table info
SHOW TABLES;
DESCRIBE sales_data;

-- Query 1: Sample data
SELECT 'QUERY 1: SAMPLE DATA' as query_title;
SELECT * FROM sales_data LIMIT 5;

-- Query 2: Sales by product
SELECT 'QUERY 2: SALES BY PRODUCT' as query_title;
SELECT 
    product_name,
    SUM(quantity) as total_quantity,
    ROUND(SUM(quantity * price), 2) as total_revenue,
    COUNT(*) as transaction_count
FROM sales_data
GROUP BY product_name
ORDER BY total_revenue DESC;

-- Query 3: Daily sales
SELECT 'QUERY 3: DAILY SALES' as query_title;
SELECT 
    sale_date,
    SUM(quantity) as daily_quantity,
    ROUND(SUM(quantity * price), 2) as daily_revenue
FROM sales_data
GROUP BY sale_date
ORDER BY sale_date;

-- Query 4: Statistics summary
SELECT 'QUERY 4: STATISTICS SUMMARY' as query_title;
SELECT 
    COUNT(*) as total_transactions,
    SUM(quantity) as total_items_sold,
    ROUND(SUM(quantity * price), 2) as total_revenue,
    ROUND(AVG(price), 2) as average_price,
    COUNT(DISTINCT product_name) as unique_products
FROM sales_data;