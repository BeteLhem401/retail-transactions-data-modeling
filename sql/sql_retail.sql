-- Reset database to ensure a clean and reproducible setup
DROP DATABASE IF EXISTS retail_analytics;
CREATE DATABASE retail_analytics;
USE retail_analytics;

-- Fact table storing one row per retail transaction
CREATE TABLE fact_transactions (
    transaction_id BIGINT PRIMARY KEY,
    transaction_date DATETIME NOT NULL,
    customer_name VARCHAR(255),
    total_items INT,
    total_cost DECIMAL(10,2),
    payment_method VARCHAR(50),
    city VARCHAR(100),
    store_type VARCHAR(50),
    discount_applied BOOLEAN,
    customer_category VARCHAR(50),
    season VARCHAR(20),
    promotion VARCHAR(100),
    product_count INT
);

-- Product dimension containing unique product names
CREATE TABLE dim_products (
    product_id INT AUTO_INCREMENT PRIMARY KEY,
    product_name VARCHAR(255) UNIQUE
);

-- Bridge table to support transactions with multiple products
CREATE TABLE fact_transaction_products (
    transaction_id BIGINT,
    product_id INT,
    PRIMARY KEY (transaction_id, product_id),
    FOREIGN KEY (transaction_id) REFERENCES fact_transactions(transaction_id),
    FOREIGN KEY (product_id) REFERENCES dim_products(product_id)
);

-- Indexes added to improve query and dashboard performance
CREATE INDEX idx_transaction_date ON fact_transactions(transaction_date);
CREATE INDEX idx_city ON fact_transactions(city);
CREATE INDEX idx_store_type ON fact_transactions(store_type);
CREATE INDEX idx_customer_category ON fact_transactions(customer_category);
CREATE INDEX idx_total_cost ON fact_transactions(total_cost);
CREATE INDEX idx_product_id ON fact_transaction_products(product_id);

-- Load cleaned transaction data and normalize boolean values
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/cleaned_retail.csv'
INTO TABLE fact_transactions
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
    transaction_id,
    transaction_date,
    customer_name,
    @products,
    total_items,
    total_cost,
    payment_method,
    city,
    store_type,
    @discount_applied,
    customer_category,
    season,
    promotion,
    product_count
)
SET discount_applied =
    CASE
        WHEN LOWER(@discount_applied) = 'true' THEN 1
        ELSE 0
    END;

-- Load product dimension generated during Python ETL
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/dim_products.csv'
INTO TABLE dim_products
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(product_name);

-- Load transaction–product relationships and ignore duplicates
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/fact_transaction_products.csv'
IGNORE
INTO TABLE fact_transaction_products
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(transaction_id, product_id);

-- Basic validation checks
SELECT COUNT(*) AS total_transactions FROM fact_transactions;
SELECT COUNT(*) AS total_products FROM dim_products;
SELECT COUNT(*) AS product_links FROM fact_transaction_products;

-- Check for broken references between tables
SELECT COUNT(*) AS orphan_links
FROM fact_transaction_products ftp
LEFT JOIN fact_transactions ft
    ON ftp.transaction_id = ft.transaction_id
WHERE ft.transaction_id IS NULL;

-- Executive-level KPIs used directly in Power BI
CREATE OR REPLACE VIEW vw_exec_kpis AS
SELECT
    COUNT(*) AS total_transactions,
    ROUND(SUM(total_cost), 2) AS total_revenue,
    ROUND(AVG(total_cost), 2) AS avg_order_value,
    SUM(total_items) AS total_items_sold
FROM fact_transactions;

-- Monthly revenue and transaction trends
CREATE OR REPLACE VIEW vw_monthly_sales AS
SELECT
    DATE_FORMAT(transaction_date, '%Y-%m') AS month,
    COUNT(*) AS transactions,
    ROUND(SUM(total_cost), 2) AS revenue
FROM fact_transactions
GROUP BY month
ORDER BY month;

-- Store-level performance comparison
CREATE OR REPLACE VIEW vw_store_performance AS
SELECT
    store_type,
    COUNT(*) AS transactions,
    ROUND(SUM(total_cost), 2) AS revenue,
    ROUND(AVG(total_cost), 2) AS avg_order_value
FROM fact_transactions
GROUP BY store_type;

-- Top products by revenue contribution
CREATE OR REPLACE VIEW vw_top_products AS
SELECT
    p.product_name,
    COUNT(*) AS times_purchased,
    ROUND(SUM(ft.total_cost), 2) AS revenue
FROM fact_transaction_products ftp
JOIN dim_products p
    ON ftp.product_id = p.product_id
JOIN fact_transactions ft
    ON ftp.transaction_id = ft.transaction_id
GROUP BY p.product_name
ORDER BY revenue DESC
LIMIT 20;

-- List all analytical views available for reporting
SHOW FULL TABLES WHERE TABLE_TYPE = 'VIEW';
