TRUNCATE TABLE bronze.crm_cust_info;
GO
BULK INSERT bronze.crm_cust_info
FROM 'D:\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);

GO

TRUNCATE TABLE bronze.crm_prd_info ;
GO
BULK INSERT bronze.crm_prd_info 
FROM 'D:\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);

GO

TRUNCATE TABLE bronze.crm_sales_details;
GO
BULK INSERT bronze.crm_sales_details
FROM 'D:\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);

GO

TRUNCATE TABLE bronze.erp_cust_az12;
GO
BULK INSERT bronze.erp_cust_az12
FROM 'D:\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);

GO

TRUNCATE TABLE bronze.erp_loc_a101;
GO
BULK INSERT bronze.erp_loc_a101
FROM 'D:\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);

GO 

TRUNCATE TABLE bronze.erp_px_cat_g1v2;
GO
BULK INSERT bronze.erp_px_cat_g1v2
FROM 'D:\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR =',',
	TABLOCK
);
