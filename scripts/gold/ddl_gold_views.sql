/*
-- ==============================
-- DDL Script: Create Gold Views
-- ==============================

Script Purpose: 
  This script creates views for the Gold Layer in data warehouse.
  The gold layer represents the final dimension and fact tables (Star Schema)

Usage:
  - These views can queried directly for analytics and reporting.
=====================================================================
*/

-- create customer dimension view

CREATE OR ALTER VIEW gold.dim_customers AS
SELECT
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key, -- surrogate key: system-generated unique identifier assigned to each record in a table
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number, 
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gndr = 'n/a' THEN COALESCE(gen,'n/a') ELSE cst_gndr END AS gender,
	ca.bdate AS birthdate,
	ci.cst_create_date AS created_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON		  ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON		  ci.cst_key = la.cid;


-- create product dimension view

CREATE OR ALTER VIEW gold.dim_products AS
SELECT
	ROW_NUMBER() OVER (ORDER BY pi.prd_start_dt, pi.prd_key) AS product_key, -- surrogate key: system-generated unique identifier assigned to each record in a table
	pi.prd_id AS product_id,
	pi.prd_key AS product_number,
	pi.prd_nm AS product_name,
	pi.cat_id AS category_id,
	COALESCE(pc.cat,'n/a') AS category,
	COALESCE(pc.subcat, 'n/a') AS subcategory,
	COALESCE(pc.maintenance,'n/a') AS maintenance,
	pi.prd_cost AS product_cost,
	pi.prd_line AS product_line,
	pi.prd_start_dt AS start_date
FROM silver.crm_prod_info pi
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pi.cat_id = pc.id
WHERE pi.prd_end_dt IS NULL; -- filter active items


-- create sales details fact table view

CREATE OR ALTER VIEW gold.fact_sales_details AS
SELECT
	sd.sls_ord_num AS order_number,
	pr.product_key,
	cx.customer_key,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS shipping_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales_amount,
	sd.sls_quantity AS quantity,
	sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cx
ON sd.sls_cust_id = cx.customer_id;


-- ===================================
-- Foreign Key Integrity (Dimensions)
-- ===================================

SELECT *
FROM gold.fact_sales_details f
LEFT JOIN gold.dim_customers c
ON f.customer_key = c.customer_key
LEFT JOIN gold.dim_products p
ON f.product_key = p.product_key
WHERE p.product_key IS NULL;

-- SELECT * FROM gold.fact_sales_details;
