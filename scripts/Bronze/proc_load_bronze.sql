/*
=============================================================================
Stored Procedure: Load Bronze Layer(source-> Bronze)
=============================================================================
Script Purpose:
      This Stored Procedure loads data into the 'bronze' schema from 
      external csv files.
      It performs the following actions:
      - Truncates the bronze tables before loading data.
      - Uses the 'BULK INSERT' command to load data from csv files
        to bronze tables.

Parameters:
      None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
      EXEC bronze.load_bronze;
==============================================================================
*/

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
