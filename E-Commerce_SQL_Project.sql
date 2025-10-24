-- ===========================================
-- Data Analyst SQL Portfolio Project
-- Dataset: E-Commerce Data (Kaggle: carrie1)
-- DB: MySQL 8.0+
-- ===========================================

-- ===========================================
-- 0) Setup: create database and use it
-- ===========================================
CREATE DATABASE IF NOT EXISTS ecommerce_proj;
USE ecommerce_proj;

-- ===========================================
-- 1) Create tables if they do not exist
-- ===========================================
CREATE TABLE IF NOT EXISTS raw_transactions (
  invoice_no VARCHAR(50),
  stock_code VARCHAR(50),
  description TEXT,
  quantity INT,
  invoice_date VARCHAR(100), -- imported as text, parse later
  unit_price DECIMAL(10,4),
  customer_id VARCHAR(50),
  country VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS staging (
  invoice_no VARCHAR(50),
  stock_code VARCHAR(50),
  description TEXT,
  quantity INT,
  invoice_datetime DATETIME,
  unit_price DECIMAL(10,4),
  customer_id VARCHAR(50),
  country VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS customers (
  customer_id INT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS customer_countries (
  customer_id INT,
  country VARCHAR(100),
  PRIMARY KEY (customer_id, country),
  FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

CREATE TABLE IF NOT EXISTS products (
  product_id VARCHAR(50) PRIMARY KEY,
  product_name TEXT,
  avg_price DECIMAL(10,4)
);

CREATE TABLE IF NOT EXISTS orders (
  order_id BIGINT AUTO_INCREMENT PRIMARY KEY,
  invoice_no VARCHAR(50) UNIQUE,
  customer_id INT,
  order_date DATETIME,
  order_status VARCHAR(50),
  total_amount DECIMAL(12,2)
);

CREATE TABLE IF NOT EXISTS order_items (
  order_item_id BIGINT AUTO_INCREMENT PRIMARY KEY,
  order_id BIGINT,
  product_id VARCHAR(50),
  quantity INT,
  unit_price DECIMAL(10,4),
  line_total DECIMAL(12,2),
  FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

CREATE TABLE IF NOT EXISTS payments (
  payment_id BIGINT AUTO_INCREMENT PRIMARY KEY,
  order_id BIGINT,
  payment_date DATETIME,
  payment_type VARCHAR(50),
  amount DECIMAL(12,2),
  FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

-- ===========================================
-- 2) Safe reset of tables
-- ===========================================

SET FOREIGN_KEY_CHECKS = 0;

TRUNCATE TABLE order_items;
TRUNCATE TABLE payments;
TRUNCATE TABLE orders;
TRUNCATE TABLE customer_countries;
TRUNCATE TABLE customers;
TRUNCATE TABLE products;
TRUNCATE TABLE staging;

SET FOREIGN_KEY_CHECKS = 1;

-- ===========================================
-- 3) Load data into staging from raw_transactions
-- ===========================================
INSERT INTO staging
SELECT
    invoice_no,
    stock_code,
    description,
    quantity,
    STR_TO_DATE(invoice_date, '%m/%d/%Y %k:%i') AS invoice_datetime,
    unit_price,
    NULLIF(TRIM(customer_id), '') AS customer_id,
    country
FROM raw_transactions
WHERE invoice_no IS NOT NULL
  AND invoice_no NOT LIKE 'C%'
  AND quantity > 0
  AND unit_price >= 0;

-- ===========================================
-- 4) Populate normalized tables
-- ===========================================
-- Customers
INSERT INTO customers (customer_id)
SELECT DISTINCT CAST(customer_id AS UNSIGNED)
FROM staging
WHERE customer_id IS NOT NULL;

-- Customer countries
INSERT INTO customer_countries (customer_id, country)
SELECT DISTINCT CAST(customer_id AS UNSIGNED), country
FROM staging
WHERE customer_id IS NOT NULL;

-- Products
INSERT INTO products (product_id, product_name, avg_price)
SELECT stock_code, MIN(description), ROUND(AVG(unit_price),4)
FROM staging
GROUP BY stock_code;

-- Orders
INSERT INTO orders (invoice_no, customer_id, order_date, order_status, total_amount)
SELECT
  invoice_no,
  CAST(MIN(customer_id) AS UNSIGNED),
  MIN(invoice_datetime),
  'complete',
  ROUND(SUM(quantity * unit_price),2)
FROM staging
GROUP BY invoice_no;

-- Order items
INSERT INTO order_items (order_id, product_id, quantity, unit_price, line_total)
SELECT o.order_id, s.stock_code, s.quantity, s.unit_price, ROUND(s.quantity*s.unit_price,2)
FROM staging s
JOIN orders o ON s.invoice_no = o.invoice_no;

-- Payments
INSERT INTO payments (order_id, payment_date, payment_type, amount)
SELECT order_id, order_date, 'card', total_amount FROM orders;

-- ===========================================
-- 5) Queries
-- ===========================================

-- Total orders per customer
SELECT c.customer_id, cc.country, COUNT(o.order_id) AS order_count
FROM customers c
JOIN customer_countries cc ON c.customer_id = cc.customer_id
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, cc.country;

-- Top 5 products by revenue
SELECT p.product_id, p.product_name, SUM(oi.line_total) AS total_revenue
FROM order_items oi
JOIN products p ON oi.product_id = p.product_id
GROUP BY p.product_id, p.product_name
ORDER BY total_revenue DESC
LIMIT 5;

-- Top 3 customers by spend per country
WITH cust_totals AS (
  SELECT cc.country, o.customer_id, SUM(o.total_amount) AS total_spend
  FROM orders o
  JOIN customer_countries cc ON o.customer_id = cc.customer_id
  GROUP BY cc.country, o.customer_id
)
SELECT * FROM (
  SELECT country, customer_id, total_spend,
         ROW_NUMBER() OVER (PARTITION BY country ORDER BY total_spend DESC) AS rn
  FROM cust_totals
) ranked
WHERE rn <= 3;

-- Cumulative monthly revenue
WITH monthly AS (
  SELECT 
    DATE_FORMAT(order_date, '%Y-%m-01') AS month_key,         -- safe for grouping/sorting
    DATE_FORMAT(order_date, '01/%m/%Y') AS month_start_uk,    -- UK format for display
    SUM(total_amount) AS revenue
  FROM orders
  GROUP BY month_key, month_start_uk
)
SELECT 
  month_start_uk AS month_start, 
  revenue,
  SUM(revenue) OVER (ORDER BY month_key) AS cumulative_revenue
FROM monthly
ORDER BY month_key;

-- Loyal customers (>=3 orders last 6 months)
-- Find the max order date
SELECT MAX(order_date) FROM orders;

WITH recent_orders AS (
  SELECT customer_id
  FROM orders
  WHERE order_date >= DATE_SUB('2011-12-09', INTERVAL 6 MONTH)
)
SELECT customer_id, COUNT(*) AS order_count
FROM recent_orders
GROUP BY customer_id
HAVING COUNT(*) >= 3
ORDER BY order_count DESC;


-- Churned customers (>12 months since first order, no orders last 6 months)
WITH first_last AS (
  SELECT 
    customer_id, 
    MIN(order_date) AS first_order, 
    MAX(order_date) AS last_order
  FROM orders
  GROUP BY customer_id
)
SELECT 
  customer_id,
  first_order,
  last_order,
  DATEDIFF(last_order, first_order) AS days_between
FROM first_last
WHERE first_order <= DATE_SUB(CURDATE(), INTERVAL 12 MONTH)
  AND last_order <= DATE_SUB(CURDATE(), INTERVAL 6 MONTH);
