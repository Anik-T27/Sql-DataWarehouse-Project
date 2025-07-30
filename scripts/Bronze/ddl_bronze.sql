/*
========================================================================
DDL Script: Creating Bronze Tables
========================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing
    tables if they already exists.
    Run this script to re-define the DDL structure of 'bronze' tables
========================================================================
*/


IF OBJECT_ID('bronze.crm_cust_info','U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;
GO
  
CREATE TABLE bronze.crm_cust_info(
cst_id INT,
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lastname NVARCHAR(50),
cst_marital_status NVARCHAR(50),
cst_gndr NVARCHAR(50),
cst_create_date DATE
);
GO

IF OBJECT_ID('bronze.crm_prd_info','U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info;
GO
CREATE TABLE bronze.crm_prd_info(
prd_id INT,
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost INT,
prd_line NVARCHAR(50),
prd_start_dt DATETIME,
prd_end_dt DATETIME
);
GO
  
IF OBJECT_ID('bronze.crm_sales_details','U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;
GO
CREATE TABLE bronze.crm_sales_details(
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT
);
GO
  
IF OBJECT_ID('bronze.erp_cust_az12','U') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12;
GO
CREATE TABLE bronze.erp_cust_az12(
cid NVARCHAR(50),
bdate DATE,
gen NVARCHAR(50)
);
GO
  
IF OBJECT_ID('bronze.erp_loc_a101','U') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101;
GO
CREATE TABLE bronze.erp_loc_a101(
cid NVARCHAR(50),
cntry NVARCHAR(50)
);
GO
  

IF OBJECT_ID('bronze.erp_px_cat_g1v2','U') IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2;
GO
CREATE TABLE bronze.erp_px_cat_g1v2(
id NVARCHAR(50),
cat NVARCHAR(50),
subcat NVARCHAR(50),
maintenance NVARCHAR(50)
);
GO
  
CREATE OR ALTER PROCEDURE bronze.load_bronze
AS

BEGIN

	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	
	BEGIN TRY
		SET @batch_start_time= GETDATE();
		PRINT('======================================');
		PRINT('LOADING BRONZE LAYER');
		PRINT('======================================');

		PRINT('--------------------------------------');
		PRINT('LOADING DATA FROM CRM SOURCE SYSTEM');
		PRINT('--------------------------------------');

		SET @start_time= GETDATE();
		PRINT('>> TRUNCATING TABLE: bronze.crm_cust_info');
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT('>> INSERTING BULK DATA INTO TABLE: bronze.crm_cust_info FROM cust_info.csv');
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\msi india\Desktop\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH(
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
		);
		SET @end_time= GETDATE();

		PRINT'>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'Seconds';
		PRINT'--------------------------------------------------------------------------------------------'



		SET @start_time= GETDATE();
		PRINT('>> TRUNCATING TABLE: bronze.crm_prd_info');
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT('>> INSERTING BULK DATA INTO TABLE: bronze.crm_prd_info FROM prd_info.csv');
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\msi india\Desktop\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
		);
		SET @end_time= GETDATE();

		PRINT'>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'Seconds';
		PRINT'--------------------------------------------------------------------------------------------'



		SET @start_time= GETDATE();
		PRINT('>> TRUNCATING TABLE: bronze.crm_sales_details');
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT('>> INSERTING BULK DATA INTO TABLE: bronze.crm_sales_details FROM sales_details.csv');
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\msi india\Desktop\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
		);
		SET @end_time= GETDATE();



		PRINT'>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'Seconds';
		PRINT'--------------------------------------------------------------------------------------------'



		PRINT('--------------------------------------');
		PRINT('LOADING DATA FROM ERP SOURCE SYSTEM');
		PRINT('--------------------------------------');


		SET @start_time= GETDATE();
		PRINT('>> TRUNCATING TABLE: bronze.erp_cust_az12');
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT('>> INSERTING BULK DATA INTO TABLE: bronze.erp_cust_az12 FROM CUST_AZ12.csv');
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\msi india\Desktop\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH(
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
		);
		SET @end_time= GETDATE();

		PRINT'>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'Seconds';
		PRINT'--------------------------------------------------------------------------------------------'


		SET @start_time= GETDATE();
		PRINT('>> TRUNCATING TABLE: bronze.erp_loc_a101');
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT('>> INSERTING BULK DATA INTO TABLE: bronze.erp_loc_a101 FROM LOC_A101.csv');
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\msi india\Desktop\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH(
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
		);
		SET @end_time= GETDATE();

		PRINT'>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'Seconds';
		PRINT'--------------------------------------------------------------------------------------------'


		SET @start_time= GETDATE();
		PRINT('>> TRUNCATING TABLE: bronze.erp_px_cat_g1v2');
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT('>>  INSERTING BULK DATA INTO TABLE: bronze.erp_px_cat_g1v2 FROM PX_CAT_G1V2.csv');
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\msi india\Desktop\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
		);
		SET @end_time= GETDATE();

		PRINT'>> Load Duration:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'Seconds';
		PRINT'--------------------------------------------------------------------------------------------'

		SET @batch_end_time= GETDATE();
		PRINT'==================================================================';
		PRINT'Loading Bronze layer is Completed';
		PRINT'Total Load Duration:' +CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) +'Seconds';
		PRINT'==================================================================';


	END TRY

	BEGIN CATCH
		
		PRINT '==============================================================';
		PRINT 'Error Occured during loading Bronze layer';
		PRINT 'Error Message'+ ERROR_MESSAGE();
		PRINT 'Error Message'+ CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR Message'+CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '==============================================================';

	END CATCH

END

EXEC bronze.load_bronze
