/*
========================================================================
Stored Procedure: Load Silver layer( Bronze-> Silver)
========================================================================
Script Purpose:
    This stored procedure performs the ETL(Extract, Transform, Load) process
    to populate the 'silver' schema tables from the 'bronze' schema.

Actions performed:
    -Truncates silver tables.
    -Inserts the transformed and cleansed data from bronze into silver tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any value.

Usage Examples:
    EXEC silver.load_silver;
=========================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN

	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time= GETDATE();
		PRINT('======================================');
		PRINT('LOADING silver LAYER');
		PRINT('======================================');

		PRINT('--------------------------------------');
		PRINT('LOADING DATA FROM bronze.crm layer');
		PRINT('--------------------------------------');

		SET @start_time= GETDATE();
		PRINT'Truncating Table:silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT'Inserting Data into: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date)

		SELECT 
		cst_id, cst_key,
		TRIM(cst_firstname) AS cst_firstname,-- 2nd Transformation-> To remove white spaces from first and last name
		TRIM(cst_lastname) AS cst_lastname,
		CASE WHEN UPPER(TRIM(cst_marital_status))='M' THEN 'Married'
			 WHEN UPPER(TRIM(cst_marital_status))='S' THEN 'Single'
			 ELSE 'N/A'
		END AS cst_marital_status,
		CASE WHEN UPPER(TRIM(cst_gndr))='M' THEN 'Male'-- 3rd Transformation-> To handle Nulls and adding fullform after checking for distinct values in the cst_gndr and cst_marital_status columns
			 WHEN UPPER(TRIM(cst_gndr)) ='F' THEN 'Female'
			 ELSE 'N/A'
		END AS cst_gndr,
		cst_create_date
		FROM (

			SELECT *, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC)AS flag_last
			FROM bronze.crm_cust_info
		) as t
		WHERE flag_last=1; -- 1st Transformation->  It returns only unique cst_id by selecting only the latest value among the duplicates

		SET @end_time= GETDATE();

		PRINT'>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'Seconds';
		PRINT'--------------------------------------------------------------------------------------------'


		SET @start_time= GETDATE();
		PRINT'Truncating Table:silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT'Inserting Data into: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info(
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
		)
		SELECT 
		prd_id,
		REPLACE(SUBSTRING(prd_key,1,5),'-', '_') AS cat_id,-- 1st Transformation- Splitting the column prd_key to cat_id and prd_key2 for joining with other tables
		SUBSTRING(prd_key, 7, len(prd_key)) AS prd_key,
		prd_nm,
		COALESCE(prd_cost,0) AS prd_cost,--2nd transformation- Handling nulls and check for negative costs
		CASE UPPER(TRIM(prd_line))-- 3rd Transformation- Data Standardization
			 WHEN 'M' THEN 'Mountain'
			 WHEN 'R' THEN 'Road'
			 WHEN 'S' THEN 'Other Sales'
			 WHEN 'T' THEN 'Touring'
			 ELSE 'N/A'
		END AS prd_line,
		CAST(prd_start_dt AS DATE) AS prd_start_dt,-- 4th Transformation- Changing Datatype from Datetime to Date
		CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt-- 5th Transformation- Calculate end date as one day before the next start date
		FROM bronze.crm_prd_info

		SET @end_time= GETDATE();

		PRINT'>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'Seconds';
		PRINT'--------------------------------------------------------------------------------------------'



		SET @start_time= GETDATE();
		PRINT'Truncating Table:silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT'Inserting Data into: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details(
		sls_ord_num, sls_prd_key, sls_cust_id, 
		sls_order_dt, sls_ship_dt, sls_due_dt, 
		sls_sales, sls_quantity, sls_price
		)
		SELECT 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE WHEN sls_order_dt=0 OR LEN(sls_order_dt) != 8 THEN NULL -- 1st Transformation- Handling invalid data
			 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)   -- 2nd - Data type casting
		END AS sls_order_dt,
		CASE WHEN sls_ship_dt=0 OR LEN(sls_ship_dt) != 8 THEN NULL  
			 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt,
		CASE WHEN sls_due_dt=0 OR LEN(sls_due_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END AS sls_due_dt,
		CASE WHEN sls_sales IS NULL OR sls_sales<=0 OR
				  sls_sales != sls_quantity* ABS(sls_price)-- Handling negative values using ABS, 
				  THEN sls_quantity * ABS(sls_price)
			 ELSE sls_sales
		END AS sls_sales,
		sls_quantity,
		CASE WHEN sls_price IS NULL OR sls_price<=0 
				  THEN sls_sales / (sls_quantity)
			 ELSE sls_price
		END AS sls_price
		FROM bronze.crm_sales_details;

		SET @end_time= GETDATE();

		PRINT'>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'Seconds';
		PRINT'--------------------------------------------------------------------------------------------'

		PRINT('--------------------------------------');
		PRINT('LOADING DATA FROM bronze.erp layer');
		PRINT('--------------------------------------');


		
		SET @start_time= GETDATE();
		PRINT'Truncating Table:silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT'Inserting Data into: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12(
		cid,
		bdate,
		gen)

		SELECT 
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, len(cid))-- 1st Transformation- Removed 'NAS' prefix from cid to match it with cst_key of silver.crm_cust_info
			 ELSE cid
		END AS cid,
		CASE WHEN bdate> GETDATE() THEN NULL-- 2nd Transformation- Replaced all the bdate which are > then present day i.e future bdate with NULL
			 ELSE bdate
		END AS bdate,
		CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'-- 3rd Transformation- Checked the distinct values of gen, then substituting short forms with full form and repcaing blank and nulls wit N/A
			 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
			 ELSE 'N/A'
		END AS gen
		FROM bronze.erp_cust_az12;

		SET @end_time= GETDATE();

		PRINT'>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'Seconds';
		PRINT'--------------------------------------------------------------------------------------------'



		SET @start_time= GETDATE();
		PRINT'Truncating Table:silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT'Inserting Data into: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101(
		cid,
		cntry)
		SELECT 
		REPLACE(cid,'-','') AS cid,-- 1st Transformation- replaced '-' in cid with '' so as to match the cst_key in silver.crm_cust_info
		CASE WHEN TRIM(cntry) IN ('USA','US') THEN 'United States'-- 2nd Transformation- Data Normalization->Replace short forms with full form, replace NULL and blank spaces with N/A
			 WHEN cntry='DE' THEN 'Germany'
			 WHEN cntry IS NULL OR cntry=' ' THEN 'N/A'
			 ELSE TRIM(cntry)-- 3rd Transformation- Trimming the column for any white spaces
		END AS cntry
		FROM bronze.erp_loc_a101;

		SET @end_time= GETDATE();

		PRINT'>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'Seconds';
		PRINT'--------------------------------------------------------------------------------------------'


		SET @start_time= GETDATE();
		PRINT'Truncating Table:silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT'Inserting Data into: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2(
		id,cat,subcat,maintenance)
		SELECT 
		id,
		cat,
		subcat,
		maintenance
		FROM bronze.erp_px_cat_g1v2;

		SET @end_time= GETDATE();

		PRINT'>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'Seconds';
		PRINT'--------------------------------------------------------------------------------------------'

		SET @batch_end_time= GETDATE();
		PRINT'==================================================================';
		PRINT'Loading silver layer is Completed';
		PRINT'Total Load Duration:' +CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) +'Seconds';
		PRINT'==================================================================';
	
	END TRY

	BEGIN CATCH
		
		PRINT '==============================================================';
		PRINT 'Error Occured during loading silver layer';
		PRINT 'Error Message'+ ERROR_MESSAGE();
		PRINT 'Error Message'+ CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR Message'+CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '==============================================================';

	END CATCH

END
