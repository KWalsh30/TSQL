# TSQL
-- TSQL - Step 1 - Clean and Stage the Raw Data
IF OBJECT_ID('Orders_Cleaned', 'U') IS NOT NULL DROP TABLE Orders_Cleaned;

SELECT 
    -- Standardize IDs and Strings
    LTRIM(RTRIM(CAST(Order_ID AS NVARCHAR(255)))) AS Order_ID,
    ROW_NUMBER() OVER (PARTITION BY Order_ID ORDER BY Order_Date) AS order_line_number,
    
    -- Date Normalization & Feature Extraction
    CAST(Order_Date AS DATE) AS order_date,
    CAST(Ship_Date AS DATE) AS ship_date,
    DATEDIFF(day, CAST(Order_Date AS DATE), CAST(Ship_Date AS DATE)) AS days_to_ship,
    YEAR(Order_Date) AS order_year,
    DATEPART(quarter, Order_Date) AS order_quarter,
    MONTH(Order_Date) AS order_month,
    DAY(Order_Date) AS order_day,
    DATENAME(weekday, Order_Date) AS order_day_of_week,
    
    -- Categorical Data
    LTRIM(RTRIM(CAST(Ship_Mode AS NVARCHAR(255)))) AS ship_mode,
    LTRIM(RTRIM(CAST(Customer_ID AS NVARCHAR(255)))) AS customer_id,
    LTRIM(RTRIM(CAST(Customer_Name AS NVARCHAR(255)))) AS customer_name,
    LTRIM(RTRIM(CAST(Segment AS NVARCHAR(255)))) AS segment,
    LTRIM(RTRIM(CAST(Country AS NVARCHAR(255)))) AS country,
    LTRIM(RTRIM(CAST(City AS NVARCHAR(255)))) AS city,
    LTRIM(RTRIM(CAST(State AS NVARCHAR(255)))) AS state,
    LTRIM(RTRIM(CAST(Postal_Code AS NVARCHAR(255)))) AS postal_code,
    LTRIM(RTRIM(CAST(Region AS NVARCHAR(255)))) AS region,
    LTRIM(RTRIM(CAST(Market AS NVARCHAR(255)))) AS market,
    LTRIM(RTRIM(CAST(Product_ID AS NVARCHAR(255)))) AS product_id,
    LTRIM(RTRIM(CAST(Category AS NVARCHAR(255)))) AS category,
    LTRIM(RTRIM(CAST(Sub_Category AS NVARCHAR(255)))) AS sub_category,
    LTRIM(RTRIM(CAST(Product_Name AS NVARCHAR(255)))) AS product_name,
    
    -- Financials & Calculated Metrics
    COALESCE(Sales, 0) AS sales,
    COALESCE(Quantity, 0) AS quantity,
    COALESCE(Discount, 0) AS discount,
    COALESCE(Profit, 0) AS profit,
    COALESCE(Shipping_cost, 0) AS shipping_cost,
    CASE WHEN COALESCE(Quantity, 0) > 0 THEN COALESCE(Sales, 0) / Quantity ELSE 0 END AS unit_price,
    CASE WHEN COALESCE(Sales, 0) > 0 THEN (COALESCE(Profit, 0) / Sales) * 100 ELSE 0 END AS profit_margin_percent,
    LTRIM(RTRIM(CAST(order_priority AS NVARCHAR(255)))) AS order_priority
INTO Orders_Cleaned 
FROM dbo.Global_Superstore
WHERE Sales IS NOT NULL AND Quantity > 0 AND Order_Date IS NOT NULL;

-- Optimize for downstream joins
CREATE CLUSTERED INDEX IX_orders_cleaned_order_date ON dbo.orders_cleaned (order_date);

-- TSQL - Step 2 - **Star Schema** - Create Dimension and Fact tables for optimal performance

-- 1. Date Dimension Table
IF OBJECT_ID('dim_date', 'U') IS NOT NULL DROP TABLE dim_date;

SELECT DISTINCT
    order_date as date_key,
    order_year as [year],
    order_quarter as [quarter],
    order_month as [month],
    order_month as month_number,
    FORMAT(order_date, 'yyyy-MM') as year_month,
    order_day as [day],
    order_day_of_week as day_of_week,
    CASE 
        WHEN order_day_of_week IN ('Saturday', 'Sunday') THEN 'Weekend'
        ELSE 'Weekday'
    END as weekend_flag

INTO dim_date
FROM orders_cleaned;

-- 2. Customer Dimension Table
IF OBJECT_ID('dim_customer', 'U') IS NOT NULL DROP TABLE dim_customer;

SELECT DISTINCT
    customer_id,
    customer_name,
    segment
INTO dim_customer
FROM orders_cleaned;

-- 3. Product Dimension Table
IF OBJECT_ID('dim_product', 'U') IS NOT NULL DROP TABLE dim_product;

SELECT DISTINCT
    product_id,
    product_name,
    category,
    sub_category

INTO dim_product
FROM orders_cleaned;

-- 4. Geography Dimension Table
IF OBJECT_ID('dim_geography', 'U') IS NOT NULL DROP TABLE dim_geography;

SELECT DISTINCT
    -- Using CONCAT_WS (with separator) is cleaner in modern SQL Server
    CONCAT(country, '-', state, '-', city) as geo_key,
    country,
    state,
    city,
    postal_code,
    region,
    market

INTO dim_geography
FROM orders_cleaned;

--Create the clean updated FACT Table

-- 1. Scrub the old table
IF OBJECT_ID('fact_sales', 'U') IS NOT NULL DROP TABLE fact_sales;

SELECT 
    f.order_id,
    f.order_date,
    f.ship_date,      -- <--- ADD THIS
    f.sales,
    f.profit,
    f.quantity,
    f.shipping_cost,
    f.customer_id,
    f.product_id,
    dl.geo_key AS LocationKey -- <--- ALIAS THIS to match your View logic
INTO fact_sales 
FROM orders_cleaned f
JOIN Dim_Customer dc ON f.customer_id = dc.customer_id 
JOIN Dim_Product dp ON f.product_id = dp.product_id 
JOIN Dim_Geography dl ON f.country = dl.country 
                    AND f.city = dl.city 
                    AND f.state = dl.state;

                    -- TSQL - Step 3 - -- Create Key Metrics and Aggregations

-- A -- Sales Over Time

IF OBJECT_ID('metrics_sales_over_time', 'U') IS NOT NULL DROP TABLE metrics_sales_over_time;

SELECT 
    FORMAT(order_date, 'yyyy-MM') AS year_month,
    YEAR(order_date) AS [year],
    MONTH(order_date) AS [month],
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(sales) AS total_sales,
    SUM(profit) AS total_profit,
    SUM(quantity) AS total_quantity,
    AVG(sales) AS avg_order_value,
    COUNT(DISTINCT customer_id) AS unique_customers
INTO metrics_sales_over_time
FROM fact_sales
GROUP BY FORMAT(order_date, 'yyyy-MM'), YEAR(order_date), MONTH(order_date);

-- B -- Sales By Category

IF OBJECT_ID('metrics_sales_by_category', 'U') IS NOT NULL 
    DROP TABLE metrics_sales_by_category;

-- Use a CROSS JOIN to get the grand total for the percentage calculation

SELECT 
    p.category,
    p.sub_category,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.sales) AS total_sales,
    SUM(f.profit) AS total_profit,
    SUM(f.quantity) AS total_quantity,

    -- Calculate margin per row and average the result

    AVG(f.profit / NULLIF(f.sales, 0)) AS avg_profit_margin, 
    SUM(f.sales) * 100.0 / NULLIF(t.grand_total, 0) AS sales_percentage
INTO metrics_sales_by_category
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
CROSS JOIN (SELECT SUM(sales) AS grand_total FROM fact_sales) t
GROUP BY p.category, p.sub_category, t.grand_total;

-- C -- Sales By Region

IF OBJECT_ID ('metrics_sales_by_region', 'U') IS NOT NULL 
DROP TABLE metrics_sales_by_region;

SELECT 
    g.market,
    g.region,
    g.country,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(TRY_CAST(f.sales AS decimal(18,2))) AS total_sales,
    SUM(TRY_CAST(f.profit AS decimal(18,2))) AS total_profit,
    AVG(TRY_CAST(f.sales AS decimal(18,2))) AS avg_order_value,
    COUNT(DISTINCT f.customer_id) AS unique_customers,
    SUM(TRY_CAST(f.sales AS decimal(18,2))) * 100.0 / NULLIF(t.grand_total, 0) AS sales_percentage
INTO metrics_sales_by_region
FROM fact_sales f
JOIN dim_geography g ON TRY_CAST(f.geo_key AS bigint) = TRY_CAST(g.geo_key AS bigint)
CROSS JOIN (SELECT SUM(TRY_CAST(sales AS decimal(18,2))) AS grand_total FROM fact_sales) t
GROUP BY g.market, g.region, g.country, t.grand_total;

-- D-- Customer Acquisition

-- 1. Drop the table if it already exists
IF OBJECT_ID('metrics_customer_acquisition', 'U') IS NOT NULL 
    DROP TABLE metrics_customer_acquisition;

-- 2. Use Common Table Expressions (CTEs) for calculations
WITH customer_first_order AS (
    SELECT 
        customer_id,
        MIN(order_date) AS first_order_date,
        YEAR(MIN(order_date)) AS acquisition_year,
        MONTH(MIN(order_date)) AS acquisition_month
    FROM fact_sales
    GROUP BY customer_id
),
customer_order_count AS (
    SELECT 
        customer_id,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(sales) AS lifetime_value,
        MAX(order_date) AS last_order_date
    FROM fact_sales
    GROUP BY customer_id
)
-- 3. Final selection with SELECT INTO
SELECT 
    cfo.acquisition_year,
    cfo.acquisition_month,
    FORMAT(cfo.first_order_date, 'yyyy-MM') AS acquisition_year_month,
    COUNT(DISTINCT cfo.customer_id) AS new_customers,
    AVG(coc.lifetime_value) AS avg_customer_lifetime_value,
    AVG(coc.total_orders) AS avg_orders_per_customer
INTO metrics_customer_acquisition
FROM customer_first_order cfo
JOIN customer_order_count coc ON cfo.customer_id = coc.customer_id
GROUP BY 
    cfo.acquisition_year, 
    cfo.acquisition_month, 
    FORMAT(cfo.first_order_date, 'yyyy-MM'); -- Must repeat the expression in GROUP BY

-- E-- Customer Average Order Value
	
	-- 1. Drop the table if it already exists
IF OBJECT_ID('metrics_aov_analysis', 'U') IS NOT NULL 
    DROP TABLE metrics_aov_analysis;

-- 2. Create the table using SELECT INTO
SELECT 
    FORMAT(f.order_date, 'yyyy-MM') AS year_month,
    c.segment,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.sales) AS total_sales,
    AVG(f.sales) AS avg_order_value,
    MIN(f.sales) AS min_order_value,
    MAX(f.sales) AS max_order_value
INTO metrics_aov_analysis
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
GROUP BY 
    FORMAT(f.order_date, 'yyyy-MM'), 
    c.segment;

-- Note: ORDER BY is not used in SELECT INTO as tables are unordered sets.


-- F-- Product Performance

-- 1. Drop the table if it already exists
IF OBJECT_ID('metrics_product_performance', 'U') IS NOT NULL 
    DROP TABLE metrics_product_performance;

-- 2. Create the table using SELECT INTO
SELECT 
    p.product_id,
    p.product_name,
    p.category,
    p.sub_category,
    COUNT(DISTINCT f.order_id) AS times_ordered,
    SUM(f.quantity) AS total_quantity_sold,
    SUM(f.sales) AS total_revenue,
    SUM(f.profit) AS total_profit,
    -- Calculate the margin: (Total Profit / Total Sales) * 100
    -- Use NULLIF to prevent "Divide by Zero" errors
    (SUM(f.profit) * 100.0) / NULLIF(SUM(f.sales), 0) AS avg_profit_margin 
INTO metrics_product_performance
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY 
    p.product_id, 
    p.product_name, 
    p.category, 
    p.sub_category;


-- G-- Shipping Performance


IF OBJECT_ID('metrics_shipping_performance', 'U') IS NOT NULL 
    DROP TABLE metrics_shipping_performance;

SELECT 
    ship_mode, -- Changed underscore to space + brackets
    COUNT(*) AS total_shipments,
    AVG(CAST(days_to_ship AS FLOAT)) AS avg_days_to_ship,
    MIN(days_to_ship) AS min_days_to_ship,
    MAX(days_to_ship) AS max_days_to_ship,
    SUM(shipping_cost) AS total_shipping_cost,
    AVG(shipping_cost) AS avg_shipping_cost
INTO metrics_shipping_performance
FROM fact_sales
GROUP BY  ship_mode;

-- 1. Drop the table if it already exists
IF OBJECT_ID('metrics_customer_segments', 'U') IS NOT NULL 
    DROP TABLE metrics_customer_segments;

-- 2. Create the table
SELECT 
    c.segment,
    COUNT(DISTINCT f.customer_id) AS total_customers,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.sales) AS total_sales,
    SUM(f.profit) AS total_profit,
    AVG(f.sales) AS avg_order_value,
    -- Use NULLIF to prevent divide-by-zero errors
    SUM(f.sales) / NULLIF(COUNT(DISTINCT f.customer_id), 0) AS avg_customer_lifetime_value
INTO metrics_customer_segments
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
GROUP BY c.segment;

-- TSQL - Step 4 - -- Create Advanced Analytics for Optional Credit

-- 4.1 Year-over-Year Growth Analysis
DROP TABLE IF EXISTS metrics_yoy_growth;

WITH yearly_sales AS (
    SELECT 
        YEAR(order_date) as [year],
        SUM(sales) as total_sales,
        SUM(profit) as total_profit,
        COUNT(DISTINCT order_id) as total_orders
    FROM fact_sales
    GROUP BY YEAR(order_date)
)
SELECT 
    current_year.[year],
    current_year.total_sales,
    prior_year.total_sales as prior_year_sales,
    ((current_year.total_sales - prior_year.total_sales) / NULLIF(prior_year.total_sales, 0) * 100) as sales_growth_percent,
    current_year.total_profit,
    prior_year.total_profit as prior_year_profit,
    ((current_year.total_profit - prior_year.total_profit) / NULLIF(prior_year.total_profit, 0) * 100) as profit_growth_percent
INTO metrics_yoy_growth
FROM yearly_sales current_year
LEFT JOIN yearly_sales prior_year ON current_year.[year] = prior_year.[year] + 1;
-- Note: ORDER BY is not allowed in a SELECT INTO statement unless TOP is also used.

-- 4.2 RFM Analysis (Recency, Frequency, Monetary)
DROP TABLE IF EXISTS metrics_rfm_analysis;

WITH rfm_calc AS (
    SELECT 
        customer_id,
        DATEDIFF(DAY, MAX(order_date), GETDATE()) as recency_days,
        COUNT(DISTINCT order_id) as frequency,
        SUM(sales) as monetary
    FROM fact_sales
    GROUP BY customer_id
),
rfm_scores AS (
    SELECT 
        customer_id,
        recency_days,
        frequency,
        monetary,
        NTILE(5) OVER (ORDER BY recency_days DESC) as recency_score,
        NTILE(5) OVER (ORDER BY frequency) as frequency_score,
        NTILE(5) OVER (ORDER BY monetary) as monetary_score
    FROM rfm_calc
)
SELECT 
    customer_id,
    recency_days,
    frequency,
    monetary,
    recency_score,
    frequency_score,
    monetary_score,
    (recency_score + frequency_score + monetary_score) as rfm_total_score,
    CASE 
        WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'Champions'
        WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'Loyal Customers'
        WHEN recency_score >= 4 AND frequency_score <= 2 AND monetary_score <= 2 THEN 'New Customers'
        WHEN recency_score <= 2 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'At Risk'
        WHEN recency_score <= 2 AND frequency_score <= 2 THEN 'Lost Customers'
        ELSE 'Potential Loyalists'
    END as customer_segment
INTO metrics_rfm_analysis
FROM rfm_scores;

-- 4.3 Cohort Analysis (Customer Retention)

IF OBJECT_ID('metrics_cohort_analysis', 'U') IS NOT NULL DROP TABLE metrics_cohort_analysis;

WITH customer_first_order AS (
    -- Get the very first month each customer shopped
    SELECT 
        customer_id,
        MIN(CAST(DATEADD(MONTH, DATEDIFF(MONTH, 0, order_date), 0) AS DATE)) as cohort_month_date
    FROM fact_sales
    GROUP BY customer_id
),
customer_activity AS (
    -- Link all orders to their respective cohort
    SELECT 
        f.customer_id,
        cfo.cohort_month_date,
        CAST(DATEADD(MONTH, DATEDIFF(MONTH, 0, f.order_date), 0) AS DATE) as activity_month_date
    FROM fact_sales f
    JOIN customer_first_order cfo ON f.customer_id = cfo.customer_id
)
SELECT 
    FORMAT(cohort_month_date, 'yyyy-MM') as cohort_month,
    FORMAT(activity_month_date, 'yyyy-MM') as order_month,
    DATEDIFF(MONTH, cohort_month_date, activity_month_date) as months_since_first_order,
    COUNT(DISTINCT customer_id) as customers
INTO metrics_cohort_analysis
FROM customer_activity
GROUP BY cohort_month_date, activity_month_date
-- ORDER BY cohort_month, months_since_first_order; (Order during query, not table creation)

-- TSQL - Step 5 - -- Create Advanced Views for Power BI
DROP VIEW IF EXISTS vw_sales_master;
GO

-- 5.1 - -- Sales Master View for Power BI

CREATE VIEW vw_sales_master AS
SELECT 
    -- Fact Table Columns
    f.order_id,
    -- f.order_line_number, -- Removed as it is invalid; check for 'row_id' if needed
    f.order_date,
    f.ship_date,
    f.sales,
    f.quantity,
    f.profit,
    f.shipping_cost,
    
    
    -- Dimension Columns
    c.customer_name,
    c.segment,
    p.product_name,
    p.category,
    p.sub_category,
    g.country,
    g.state,
    g.city,
    g.region,
    g.market,
    d.[year],
    d.[quarter],
    d.[month],
    d.day_of_week,
    d.weekend_flag
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
JOIN dim_product p ON f.product_id = p.product_id
-- Using LocationKey based on your earlier successful joins
JOIN dim_geography g ON f.LocationKey = g.geo_key 
JOIN dim_date d ON f.order_date = d.date_key

GO

-- 5.2 - -- KPI Master View for Power BI

-- 1. Drop and Recreate the KPI Summary View
DROP VIEW IF EXISTS vw_kpi_summary;
GO

CREATE VIEW vw_kpi_summary AS
SELECT 
    SUM(f.sales) AS total_sales,
    SUM(f.profit) AS total_profit,
    -- Calculate margin: (Total Profit / Total Sales) * 100
    (SUM(f.profit) * 100.0) / NULLIF(SUM(f.sales), 0) AS avg_profit_margin,
    COUNT(DISTINCT f.order_id) AS total_orders,
    COUNT(DISTINCT f.customer_id) AS total_customers,
    AVG(f.sales) AS avg_order_value,
    SUM(f.quantity) AS total_quantity_sold
FROM fact_sales f; -- This was the missing piece!
GO
