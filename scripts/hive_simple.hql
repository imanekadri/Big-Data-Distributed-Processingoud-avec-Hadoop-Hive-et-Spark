-- Use default database
USE default;

-- Create Hive table
CREATE EXTERNAL TABLE IF NOT EXISTS sales (
    product_id INT,
    product_name STRING,
    quantity INT,
    price FLOAT,
    sale_date STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/tmp/datasets/'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Show table
SHOW TABLES;
DESCRIBE sales;

-- Query 1: Show sample data
SELECT * FROM sales LIMIT 5;

-- Query 2: Sales by product
SELECT product_name, SUM(quantity) AS total_quantity,
       ROUND(SUM(quantity * price), 2) AS total_revenue
FROM sales
GROUP BY product_name
ORDER BY total_revenue DESC;

-- Query 3: Daily sales
SELECT sale_date, SUM(quantity) AS daily_quantity
FROM sales
GROUP BY sale_date
ORDER BY sale_date;

-- Query 4: Statistics
SELECT COUNT(*) AS total_transactions,
       SUM(quantity) AS total_items_sold,
       ROUND(SUM(quantity * price), 2) AS total_revenue
FROM sales;
