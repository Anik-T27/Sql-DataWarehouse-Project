/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================

IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO
CREATE VIEW gold.dim_customers AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,-- Creating a surrogate key to give uniqueness to the table
	ci.cst_id AS customer_id,-- Renaming columns to friendly, meaningful names
	ci.cst_key AS customer_number,
	ci.cst_firstname AS firstname,
	ci.cst_lastname AS lastname,
	la.cntry AS country,
	ci.cst_marital_status AS marital_status,
	CASE WHEN cst_gndr!= 'N/A' THEN cst_gndr --whenever data from cst_gndr doesnt match ca.gen then we choose cst_gndr 
		 ELSE COALESCE(ca.gen, 'N/A')        -- but when we have N/A in cst_gndr then we can fill it with data from ca.gen
	END AS gender,                           -- but after coalescing nulls with N/A
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
ON ci.cst_key= ca.cid
LEFT JOIN silver.erp_loc_a101 AS la
ON ci.cst_key= la.cid;
GO

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO
CREATE VIEW gold.dim_products AS
SELECT
  ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,--surrogate key
  pn.prd_id AS product_id,
  pn.prd_key AS product_number,
  pn.prd_nm AS product_name,
  pn.cat_id AS category_id,
  pc.cat AS category,
  pc.subcat AS sub_category,
  pc.maintenance,
  pn.prd_cost AS product_cost,
  pn.prd_line AS product_line,
  pn.prd_start_dt AS start_date
FROM silver.crm_prd_info AS pn
LEFT JOIN silver.erp_px_cat_g1v2 AS pc
ON pn.cat_id= pc.id
WHERE pn.prd_end_dt IS NULL;-- Filter out historical data

GO
-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================

IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO
CREATE VIEW gold.fact_sales AS
SELECT 
  sd.sls_ord_num AS order_number,
  pr.product_key AS product_key,
  cu.customer_key AS customer_key,
  sd.sls_order_dt AS order_date,
  sd.sls_ship_dt AS shipping_date,
  sd.sls_due_dt AS due_date,
  sd.sls_sales AS sales_amount,
  sd.sls_quantity,
  sd.sls_price
FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_products AS pr 
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers AS cu
ON sd.sls_cust_id= cu.customer_id;
GO




