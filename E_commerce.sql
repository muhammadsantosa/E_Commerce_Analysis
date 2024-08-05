----------Extracting relevant data and creating a table----------

CREATE TABLE ecommerce_customer_data_large AS

SELECT * FROM ecommerce_customer_data_large01

UNION ALL

SELECT * FROM ecommerce_customer_data_large02

UNION ALL

SELECT * FROM ecommerce_customer_data_large03

UNION ALL

SELECT * FROM ecommerce_customer_data_large04

UNION ALL

SELECT * FROM ecommerce_customer_data_large05

UNION ALL

SELECT * FROM ecommerce_customer_data_large06

UNION ALL

SELECT * FROM ecommerce_customer_data_large07

UNION ALL

SELECT * FROM ecommerce_customer_data_large08

UNION ALL

SELECT * FROM ecommerce_customer_data_large09

UNION ALL

SELECT * FROM ecommerce_customer_data_large10;

----------EDA and ETL----------

-----Checking the Table format-----
PRAGMA table_info(ecommerce_customer_data_large); ---Duplicate Age

-----Making Ecommerce_Customers table-----
CREATE TABLE ecommerce_customers(
    order_id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id INT,
    customer_name VARCHAR(100),
    customer_gender VARCHAR(100),
    customer_age INT,
    purchase_date DATE,
    purchase_time TIME,
    product_category VARCHAR(100),
    product_price INT,
    quantity INT,
    total_purchase_amount INT,
    payment_method VARCHAR(100),
    returns INT,
    churn INT,
    FOREIGN KEY (customer_id) REFERENCES CLTV (customer_id)
);

INSERT INTO ecommerce_customers(
    customer_id,
    customer_name,
    customer_gender,
    customer_age,
    purchase_date,
    purchase_time,
    product_category,
    product_price,
    quantity,
    total_purchase_amount,
    payment_method,
    returns,
    churn
)

SELECT
    Customer_ID,
    Customer_Name,
    Gender,
    Customer_Age,
    DATE(Purchase_Date) AS purchase_date,
    TIME(Purchase_Date) AS purchase_time,
    Product_Category,
    Product_Price,
    Quantity,
    Total_Purchase_Amount,
    Payment_Method,
    Returns,
    Churn
FROM ecommerce_customer_data_large
ORDER BY purchase_date, purchase_time;

-----Checking new table format of ecommerce_customers-----
PRAGMA table_info(ecommerce_customers);
SELECT * FROM ecommerce_customers;

-----Checking status of non-value columns-----
SELECT 
    COUNT(*) AS DATA_COUNT,
    MAX(customer_id) as CUSTOMERS,
    MIN(purchase_date) AS EARLIEST_DATE,
    COUNT(DISTINCT product_category) AS PRODUCT_CATEGORIES,
    COUNT(DISTINCT payment_method) AS PAYMENT_METHODS,
    COUNT(DISTINCT customer_gender) AS GENDERS
FROM ecommerce_customers;

-----Checking status of Product Prices-----
SELECT
    AVG(product_price) AS AVG_PRODUCT_PRICE,
    MIN(product_price) AS MIN_PRODUCT_PRICE,
    MAX(product_price) AS MAX_PRODUCT_PRICE
FROM ecommerce_customers;

-----Checking status of Quantity-----
SELECT
    AVG(quantity) AS AVG_QUANTITY,
    MIN(quantity) AS MIN_QUANTITY,
    MAX(quantity) AS MAX_QUANTITY
FROM ecommerce_customers;

-----Checking status of Purchase Amount-----
SELECT
    AVG(total_purchase_amount) AS AVG_TOTAL_PURCHASE_AMOUNT,
    MIN(total_purchase_amount) AS MIN_TOTAL_PURCHASE_AMOUNT,
    MAX(total_purchase_amount) AS MAX_TOTAL_PURCHASE_AMOUNT
FROM ecommerce_customers;

-----Checking status of age-----
SELECT
    AVG(customer_age) AS AVG_CUSTOMER_AGE,
    MIN(customer_age) AS MIN_CUSTOMER_AGE,
    MAX(customer_age) AS MAX_CUSTOMER_AGE
FROM ecommerce_customers;

-----Checking status of returns-----
SELECT
    AVG(returns) AS AVG_RETURNS,
    MIN(returns) AS MIN_RETURNS,
    MAX(returns) AS MAX_RETURNS
FROM ecommerce_customers;

-----Checking which columns have null values-----
SELECT 'customer_id' AS null_columns
FROM ecommerce_customers
WHERE customer_id IS NULL
UNION
SELECT 'customer_name' AS cull_columns
FROM ecommerce_customers
WHERE customer_name IS NULL
UNION
SELECT 'customer_gender' AS null_columns
FROM ecommerce_customers
WHERE customer_gender IS NULL
UNION
SELECT 'customer_age' AS null_columns
FROM ecommerce_customers
WHERE customer_age IS NULL
UNION
SELECT 'purchase_date' AS null_columns
FROM ecommerce_customers
WHERE purchase_date IS NULL
UNION
SELECT 'purchase_time' AS null_columns
FROM ecommerce_customers
WHERE purchase_time IS NULL
UNION
SELECT 'product_category' AS null_columns
FROM ecommerce_customers
WHERE product_category IS NULL
UNION
SELECT 'product_price' AS null_columns
FROM ecommerce_customers
WHERE product_price IS NULL
UNION
SELECT 'quantity' AS null_columns
FROM ecommerce_customers
WHERE quantity IS NULL
UNION
SELECT 'total_purchase_amount' AS null_columns
FROM ecommerce_customers
WHERE total_purchase_amount IS NULL
UNION
SELECT 'payment_method' AS null_columns
FROM ecommerce_customers
WHERE payment_method IS NULL
UNION
SELECT 'returns' AS null_columns
FROM ecommerce_customers
WHERE returns IS NULL
UNION
SELECT 'churn' AS null_columns
FROM ecommerce_customers
WHERE churn IS NULL;

-----Ammending null values-----
SELECT
    'one_to_one' AS chances,
    (COUNT(*) * 100.0) / (SELECT COUNT(*) FROM ecommerce_customers) AS percentages
FROM ecommerce_customers
WHERE returns = 1 AND churn = 1
UNION
SELECT
    'one_to_zero' AS chances,
    (COUNT(*) * 100.0) / (SELECT COUNT(*) FROM ecommerce_customers) AS percentages
FROM ecommerce_customers
WHERE returns = 1 AND churn = 0
UNION
SELECT
    'zero_to_one' AS chances,
    (COUNT(*) * 100.0) / (SELECT COUNT(*) FROM ecommerce_customers) AS percentages
FROM ecommerce_customers
WHERE returns = 0 AND churn = 1
UNION
SELECT
    'zero_to_zero' AS chances,
    (COUNT(*) * 100.0) / (SELECT COUNT(*) FROM ecommerce_customers) AS percentages
FROM ecommerce_customers
WHERE returns = 0 AND churn = 0
ORDER BY percentages;

-----Updating null values for each case with percentages calculated based on ratios-----
UPDATE ecommerce_customers
SET returns = 
    CASE 
        WHEN churn = 1 AND RANDOM() >= 52.16 THEN 0
        WHEN churn = 1 AND RANDOM() < 52.16 THEN 1
        WHEN churn = 0 AND RANDOM() >= 50.15 THEN 1
        WHEN churn = 0 AND RANDOM() < 50.15 THEN 0
        ELSE returns
    END
WHERE returns IS NULL;

----------CLTV Customer Segmentation----------
-----Creating CLTV table and schema-----
CREATE TABLE CLTV(
    customer_id INTEGER PRIMARY KEY,
    total_purchases REAL,
    total_quantity REAL,
    total_price REAL,
    avg_order_value REAL,
    purchase_frequency REAL,
    profit_margin REAL,
    customer_value REAL,
    churn_rate REAL,
    cltv_f REAL
);

INSERT INTO CLTV(
    customer_id,
    total_purchases,
    total_quantity,
    total_price,
    avg_order_value,
    profit_margin
)

SELECT 
    customer_id,
    COUNT(*) AS total_purchases,
    SUM(quantity) AS total_quantity,
    SUM(total_purchase_amount) AS total_price,
    (SUM(total_purchase_amount)/COUNT(*)) AS avg_order_value,
    SUM(total_purchase_amount)*0.1 AS profit_margin
FROM ecommerce_customers
WHERE returns = 0
GROUP BY customer_id
ORDER BY total_purchases DESC, total_quantity DESC, total_price DESC, avg_order_value DESC, profit_margin DESC;

-----Updating purchase_frequency column-----
UPDATE CLTV
SET purchase_frequency =(
    SELECT 
        (SUM(total_purchases) / (SELECT COUNT(customer_id) FROM CLTV)) AS purchase_frequency
    FROM CLTV as sub
    WHERE sub.customer_id = CLTV.customer_id
);

-----Updating customer_value column-----
UPDATE CLTV
SET customer_value = (
    SELECT 
        avg_order_value*purchase_frequency as customer_value
    FROM CLTV as sub
    WHERE sub.customer_id = CLTV.customer_id
);

-----Updating the churn_rate column-----
CREATE TABLE cltv_churn_rate AS
SELECT
    customer_id,
    1 - CAST(
        (SELECT COUNT(DISTINCT customer_id) FROM CLTV WHERE total_purchases > 1) AS REAL
    ) / CAST((SELECT COUNT(customer_id) FROM CLTV)  AS REAL) AS churn_rate
FROM CLTV
GROUP BY customer_id;

UPDATE CLTV
SET churn_rate = (
    SELECT churn_rate
    FROM cltv_churn_rate
    WHERE cltv_churn_rate.customer_id = CLTV.customer_id
);

DROP TABLE IF EXISTS cltv_churn_rate;

-----Updating the cltv_f column-----
UPDATE CLTV
SET cltv_f = (
    SELECT (customer_value / churn_rate) * profit_margin
    FROM CLTV AS sub
    WHERE sub.customer_id = CLTV.customer_id
);

-----Ordering the cltv table-----
CREATE TABLE CLTV_temp AS
SELECT * FROM CLTV
ORDER BY cltv_f DESC;

DROP TABLE IF EXISTS CLTV;

ALTER TABLE CLTV_temp RENAME TO CLTV;

-----Categorizing-----
WITH segments AS (
    SELECT
        *,
        NTILE(4) OVER (ORDER BY cltv_f DESC) AS segment
    FROM
        CLTV
)

SELECT
    *,
    CASE
        WHEN segment = 1 THEN 'Category A'
        WHEN segment = 2 THEN 'Category B'
        WHEN segment = 3 THEN 'Category C'
        WHEN segment = 4 THEN 'Category D'
        ELSE NULL
    END AS Category
FROM
    segments;