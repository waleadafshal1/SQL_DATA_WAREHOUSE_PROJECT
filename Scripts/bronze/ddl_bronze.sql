CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
     DECLARE @Start_time DATETIME , @End_time DateTime

      BEGIN TRY 

		PRINT'====================================================================';
		PRINT'Loading Bronze Layer';
		PRINT'====================================================================';


		PRINT'--------------------------------------------';
		PRINT'Loading CRM Data Source Tables';
		PRINT'--------------------------------------------';

		SET @Start_time =GETDATE();
		-- ==================== CRM: cust_info ====================;
		PRINT'>>>Truncating  Table bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT'>>>Inserting Data Into Table bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\Mega Computer\Downloads\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',' ,
			TABLOCK
		);

		SET @End_time = GETDATE();
		   -- ==================== CRM: prd_info ====================;
        SET @Start_time =GETDATE();
		PRINT '>>> Truncating Table bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>>> Inserting Data Into Table bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\Mega Computer\Downloads\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',' ,
			TABLOCK
		);
		SET @End_time = GETDATE();
		-- ==================== CRM: sales_details ====================;
		PRINT '>>> Truncating Table bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '>>> Inserting Data Into Table bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\Mega Computer\Downloads\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',' ,
			TABLOCK
		);

		PRINT'--------------------------------------------';
		PRINT'Loading ERP Data Source Tables';
		PRINT'--------------------------------------------';

		-- ==================== ERP: cust_az12 ====================
		SET @Start_time = GETDATE();
		PRINT '>>> Truncating Table bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '>>> Inserting Data Into Table bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\Mega Computer\Downloads\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',' ,
			TABLOCK
		);
		SET @End_time = GETDATE(); 
		 -- ==================== ERP: loc_a101 ====================
		PRINT '>>> Truncating Table bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT '>>> Inserting Data Into Table bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\Mega Computer\Downloads\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',' ,
			TABLOCK
		);

		-- ==================== ERP: px_cat_g1v2 ====================
		SET @Start_time = GETDATE();
		PRINT '>>> Truncating Table bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT '>>> Inserting Data Into Table bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\Mega Computer\Downloads\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',' ,
			TABLOCK
		);
		SET @End_time = GETDATE();
		PRINT '====================================================================';
		PRINT 'Bronze Layer Load Completed Successfully!';
		PRINT '====================================================================';
	END TRY 
	BEGIN CATCH 
	    PRINT'=================================================='
		PRINT'ERROR OCURED DURING LOADING BRONZE LAYER '
		PRINT'ERROR MESSAGE:' + ERROR_Message();
		PRINT'ERROR MESSAGE:' + CAST(ERROR_NUMBER()AS NVARCHAR);
		PRINT 'Error Line     : ' + CAST(ERROR_LINE() AS NVARCHAR(10));
		PRINT'=================================================='
	END CATCH 
END 
