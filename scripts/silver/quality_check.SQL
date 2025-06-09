-- ==============================================
-- Data Quality Checking: silver.crm_cust_info
-- ==============================================

-- Check for nulls or duplicates in primary key
-- Expectation: No result
SELECT
	cst_id,
	count(*) as count
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING count(*) > 1 OR cst_id IS NULL;

-- Check for unwanted spaces
-- Expectation: No result
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

-- Data Standardization & Consistency
SELECT DISTINCT(cst_gndr)
FROM silver.crm_cust_info;

SELECT * FROM silver.crm_cust_info;


-- ==============================================
-- Data Quality Checking: silver.crm_prod_info
-- ==============================================

-- Check for nulls or duplicates in prd_id
-- Expectation: No result
SELECT
	prd_id,
	count(*) as count
FROM silver.crm_prod_info
GROUP BY prd_id
HAVING count(*) > 1;

-- Check for unwanted spaces
-- Expectation: No result
SELECT prd_nm
FROM silver.crm_prod_info
WHERE prd_nm != TRIM(prd_nm);

-- Check for negative values
-- Expectation: No result
SELECT prd_cost
FROM silver.crm_prod_info
WHERE prd_cost < 0;

-- Data Standardization & Consistency
SELECT DISTINCT(prd_line)
FROM silver.crm_prod_info;

-- Check invalid dates
-- Expectation: No result
SELECT
	*
FROM silver.crm_prod_info
WHERE prd_end_dt > prd_start_dt

-- ==============================================
-- Data Quality Checking: silver.crm_sales_details
-- ==============================================


-- Check for Invalid Dates
-- Expectation: No result
SELECT
sls_order_dt
FROM silver.crm_sales_details
WHERE
	sls_order_dt IS NULL
	OR sls_order_dt > (SELECT FORMAT(GETDATE(), 'yyyyMMdd') AS 'yyyymmdd');

-- Check for Nulls
-- Expectation: No result
SELECT sls_ord_num, sls_prd_key, sls_cust_id
FROM silver.crm_sales_details
WHERE sls_ord_num IS NULL OR sls_prd_key IS NULL OR sls_cust_id IS NULL;

-- Check Invalid Date Orders
-- Expectation: No result
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

-- Check for negative, zero and null values
-- Expectation: No result
SELECT sls_sales, sls_quantity, sls_price
FROM silver.crm_sales_details
WHERE sls_sales <= 0 
	OR sls_quantity <= 0 
	OR sls_price <= 0 
	OR sls_sales IS NULL 
	OR sls_quantity IS NULL 
	OR sls_price IS NULL;
