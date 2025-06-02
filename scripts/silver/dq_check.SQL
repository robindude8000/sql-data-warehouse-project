-- ==============================================
-- Data Quality Checking: bronze.crm_cust_info
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
-- Data Quality Checking: bronze.crm_prod_info
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
