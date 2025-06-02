/*
===== Data Ingestion =====
Script Purpose:
	To create a stored procedure that deletes and load all data
	into bronze.crm and bronze.erp tables from specific source (file path).
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();

		PRINT '==================================='
		PRINT '      Loading Bronze Layer'
		PRINT '==================================='

		PRINT '-----------------------------------'
		PRINT '      Loading CRM Tables'
		PRINT '-----------------------------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info'
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Inserting Data Into: bronze.crm_cust_info'
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\Robindude\Documents\0. SQL\Baraa\Data Warehousing\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + 'milliseconds';
		
		PRINT '-----------------------------------'
		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: bronze.crm_prod_info'
		TRUNCATE TABLE bronze.crm_prod_info;

		PRINT '>> Inserting Data Into: bronze.crm_prod_info'
		BULK INSERT bronze.crm_prod_info
		FROM 'C:\Users\Robindude\Documents\0. SQL\Baraa\Data Warehousing\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + 'milliseconds';

		PRINT '-----------------------------------'
		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: bronze.crm_sales_details'
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Inserting Data Into: bronze.crm_sales.details'
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\Robindude\Documents\0. SQL\Baraa\Data Warehousing\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + 'milliseconds';


		PRINT('-----------------------------------')
		PRINT('      Loading ERP Tables')
		PRINT('-----------------------------------')

		PRINT '-----------------------------------'
		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: bronze.erp_cust_az12'
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> Inserting Data Into: bronze.erp_cust_az12'
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\Robindude\Documents\0. SQL\Baraa\Data Warehousing\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + 'milliseconds';

		PRINT '-----------------------------------'
		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: bronze.erp_loc_a101'
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> Inserting Data Into: bronze.erp_loc_a101'
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\Robindude\Documents\0. SQL\Baraa\Data Warehousing\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + 'milliseconds';

		PRINT '-----------------------------------'
		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2'
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2'
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\Robindude\Documents\0. SQL\Baraa\Data Warehousing\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + 'milliseconds';

		SET @batch_end_time = GETDATE();

		PRINT ''
		PRINT ''
		PRINT '==================================='
		PRINT '>> BATCH TOTAL LOAD DURATION: ' + CAST(DATEDIFF(millisecond, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'milliseconds';
	END TRY
	
	-- error handling
	BEGIN CATCH
		PRINT '==================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '==================================='
	END CATCH

END

-- EXEC bronze.load_bronze;

-- SELECT * FROM bronze.crm_cust_info;
-- SELECT * FROM bronze.crm_prod_info;
-- SELECT * FROM bronze.crm_sales_details;
-- SELECT * FROM bronze.erp_cust_az12;
-- SELECT * FROM bronze.erp_loc_a101;
-- SELECT * FROM bronze.erp_px_cat_g1v2;


