/*
=========================================================
Stored Prodecure: Load Silver Layer (Bronze -> Silver)
=========================================================
Script Purpose:
  This stored procedure performs the ETL (Extract, Transform, Load) process to 
  populate the 'silver' schema tables from the 'bronze' schema.
  It performs the following actions:
  - Truncates the silver tables before loading data.
  - Insert transformed and cleaned data from Bronze into Silver tables.
Parameters:
  None
  This stored procedure does not accept any parametes or return any values.

Usage Example:
  EXEC silver.load_silver;
==========================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=====================';
		PRINT 'Loading Bronze Layer';
		PRINT '=====================';

		PRINT '------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_cust_info;
		BULK INSERT bronze.crm_cust_info
		FROM 'D:\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		TRUNCATE TABLE bronze.crm_prd_info ;
		BULK INSERT bronze.crm_prd_info 
		FROM 'D:\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		TRUNCATE TABLE bronze.crm_sales_details;
		BULK INSERT bronze.crm_sales_details
		FROM 'D:\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> CRM Load Duration: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR)+'seconds';
		

		PRINT '------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_cust_az12;
		BULK INSERT bronze.erp_cust_az12
		FROM 'D:\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		TRUNCATE TABLE bronze.erp_loc_a101;
		BULK INSERT bronze.erp_loc_a101
		FROM 'D:\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'D:\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> ERP Load Duration ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		SET @batch_end_time = GETDATE();
		PRINT '>> Bronze Layer Load Duration ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS VARCHAR) + 'seconds';
	END TRY
	BEGIN CATCH
		PRINT '=======================================================';
		PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
		PRINT 'Message    : ' + ERROR_MESSAGE();
		PRINT 'Number     : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Severity   : ' + CAST(ERROR_SEVERITY() AS NVARCHAR);
		PRINT 'State      : ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT 'Procedure  : ' + ISNULL(ERROR_PROCEDURE(), 'N/A');
		PRINT 'Line       : ' + CAST(ERROR_LINE() AS NVARCHAR);
		PRINT '=======================================================';
	END CATCH
END;
