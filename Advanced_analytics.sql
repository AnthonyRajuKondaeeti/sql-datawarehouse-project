use DataWarehouse
							-- Advanced Analytics
-- Change-over-time trends

-- Analyze Sales Performace Over Time
SELECT
	--YEAR(order_date) order_year,
	--MONTH(order_date) order_year,
	DATETRUNC(MONTH,order_date) as order_date,
	COUNT(DISTINCT customer_key) customers,
	SUM(sales_amount) total_sales
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATETRUNC(MONTH,order_date)
ORDER BY DATETRUNC(MONTH,order_date)
--GROUP BY YEAR(order_date),MONTH(order_date)
--ORDER BY YEAR(order_date),MONTH(order_date)

-- Cummulative Analysis(aggreate the data progressively over time)

-- Cal the total sales per month
-- and the running total of sales over time
SELECT 
	order_date,
	total_sales,
	SUM(total_sales) OVER(ORDER BY order_date) AS running_total_sales
FROM (
SELECT 
	DATETRUNC(MONTH, order_date) as order_date,
	SUM(sales_amount) total_sales
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATETRUNC(MONTH, order_date)
)t

select
	order_date,
	total_sales,
	sum(total_sales) over(order by order_date) as running_total_sales,
	avg_price,
	avg(avg_price) over(order by order_date) as avg_total_sales
from(
select 
	datetrunc(year,order_date) as order_date,
	sum(sales_amount) total_sales,
	avg(price) avg_price
from gold.fact_sales
where order_date is not null
group by datetrunc(year,order_date)
)t

--Performance Analysis(Comparing the current value to a target value)
/* Analyze the yearly performance of products by comparing the sales
to both the avg sales performance of the product and the prev year's sales*/
WITH yearly_product_sales AS (
	SELECT 
		YEAR(f.order_date) AS order_year,
		p.product_name,
		SUM(f.sales_amount) As current_sales
	FROM gold.fact_sales f
	LEFT JOIN gold.dim_products p
	ON f.product_key=p.product_key
	WHERE f.order_date IS NOT NULL
	GROUP BY YEAR(f.order_date),p.product_name
)
SELECT 
	order_year,
	product_name,
	current_sales,
	AVG(current_sales) OVER(PARTITION BY product_name) AS avg_sales,
	current_sales - AVG(current_sales) OVER(PARTITION BY product_name) AS diff_avg,
	CASE 
		WHEN current_sales - AVG(current_sales) OVER(PARTITION BY product_name) > 0 THEN 'Above Avg'
		WHEN current_sales - AVG(current_sales) OVER(PARTITION BY product_name) < 0 THEN 'Below Avg'
		ELSE 'Avg'
	END avg_change,
	LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) prev_year_sales,
	current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) AS diff_prev_year,
	CASE 
		WHEN current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) > 0 THEN 'Increase'
		WHEN current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) < 0 THEN 'Decrease'
		ELSE 'No Change'
	END avg_change
FROM yearly_product_sales
ORDER BY product_name, order_year

-- Part-To-Whole Analysis(how an individual part is performing compared to the overall)

-- Which categories contribute the most to overall sales?
WITH category_sales AS (
	SELECT
		p.category,
		SUM(f.sales_amount) total_sales
	FROM gold.fact_sales f
	LEFT JOIN gold.dim_products p
	ON f.product_key=p.product_key
	GROUP BY p.category
)
SELECT 
	category,
	total_sales,
	SUM(total_sales) OVER() overall_sales,
	CONCAT(ROUND(CAST(total_sales AS FLOAT)/(SUM(total_sales) OVER())*100,2), '%') AS per_of_total
FROM category_sales
ORDER BY total_sales DESC

-- Data Segmentation(Group the data based on a specific range)
/*Segment products into cost ranges and 
count how many products fall into each segment*/

WITH product_segments AS (
	SELECT 
		product_key,
		product_name,
		cost,
		CASE 
			WHEN cost<100 THEN 'Below 100'
			WHEN cost BETWEEN 100 AND 500 THEN '100-500'
			WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
			ELSE 'Above 1000'
		END cost_range
	FROM gold.dim_products 
)
SELECT 
	cost_range,
	COUNT(cost_range) AS total_products
FROM product_segments
GROUP BY cost_range
ORDER BY COUNT(cost_range) DESC

/*Group customers into three segments based on their spending behaviour:
	-VIP: customers with at least 12 months of history and spending more than 5000,
	-Regular: Customers with at least 12 months of history but spending 5000 or less.
	-New: Customers with a lifespan less than 12 months.
And find the total no. of customers by each group. */
WITH customer_spending AS (
	SELECT 
		c.customer_key,
		SUM(f.sales_amount) AS total_spending,
		MIN(f.order_date) AS first_order,
		MAX(f.order_date) AS last_order,
		DATEDIFF(MONTH, MIN(order_date),MAX(order_date)) AS active_months
	FROM gold.fact_sales f
	LEFT JOIN gold.dim_customers c ON c.customer_key = f.customer_key
	GROUP BY c.customer_key
)
SELECT 
	customer_segments,
	COUNT(customer_key) AS total_customers
FROM (
	SELECT 
		customer_key,
		CASE
			WHEN active_months >= 12 AND total_spending > 5000 THEN 'VIP'
			WHEN active_months >= 12 AND total_spending <= 5000 THEN 'Regular'
			ELSE 'New'
		END AS customer_segments
	FROM customer_spending
)t
GROUP BY customer_segments
ORDER BY total_customers DESC;

/*
=======================================================
Customer Report
=======================================================
Purpose:
	- This report consolidates key customer metrics and behaviors
Highlights:
	1. Gathers essential fields such as names, ages, and transaction details.
	2. Segments customers into categoreis (VIP, Regular, New) and age groups.
	3. Aggregates customer-level metrics:
		- total orders
		- tolatl sales
		- total quantity purchased
		- total products
		- lifespan (in months)
	4. Calculates valuable KPIs:
		- rececy (months since last order)
		- avg order value
		- avg monthly spend
	====================================================
*/
CREATE VIEW gold.report_customers AS
WITH base_query AS (
/*------------------------------------------------------
1) Base Query: Retrieves core columns from tables
--------------------------------------------------------*/
	SElECT
		f.order_number,
		f.product_key,
		f.order_date,
		f.sales_amount,
		f.quantity,
		c.customer_key,
		c.customer_number,
		CONCAT(c.first_name,' ',c.last_name) AS customer_name,
		DATEDIFF(YEAR,c.birthdate,GETDATE()) AS age
	FROM gold.fact_sales f
	LEFT JOIN gold.dim_customers c
	ON c.customer_key=f.customer_key
	WHERE f.order_date IS NOT NULL
), 
customer_aggregation AS (
/*----------------------------------------------------------------------
2) Customer Aggregations: Summarizes key metrics at the customer level
------------------------------------------------------------------------*/
	SELECT 
		customer_key,
		customer_number,
		customer_name,
		age,
		COUNT(DISTINCT order_number) AS total_orders,
		SUM(sales_amount) AS total_sales,
		SUM(quantity) AS total_quantity,
		COUNT(DISTINCT product_key) AS total_products,
		MAX(order_date) AS last_order,
		DATEDIFF(MONTH, MIN(order_date),MAX(order_date)) AS lifespan
	FROM base_query
	GROUP BY 
		customer_key,
		customer_number,
		customer_name,
		age
)
SELECT	
	customer_key,
	customer_number,
	customer_name,
	age,
	CASE 
		WHEN age<20 THEN 'Under 20'
		WHEN age<30 THEN '20-29'
		WHEN age<40 THEN '30-39'
		WHEN age<50 THEN '40-49'
		ELSE '50 and above'
	END AS age_group,
	CASE
		WHEN lifespan >= 12 AND total_sales > 5000 THEN 'VIP'
		WHEN lifespan >= 12 AND total_sales <= 5000 THEN 'Regular'
		ELSE 'New'		
	END AS customer_segment,
	last_order,
	DATEDIFF(MONTH, last_order,GETDATE()) AS recency,
	total_orders,
	total_sales,
	total_quantity,
	total_products,	
	lifespan,
	--Compute avg order value
	CASE
		WHEN total_orders=0 THEN 0
		ELSE total_sales/total_orders 
	END AS avg_order_value,
	--Compute avg monthly spend
	CASE
		WHEN lifespan=0 THEN total_sales
		ELSE total_sales/lifespan 
	END AS avg_monthly_spend
FROM customer_aggregation

SELECT * FROM gold.report_customers


