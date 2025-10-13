/*==============================================================================
  STORED PROCEDURE: bronze.load_bronze
  
  PURPOSE: Load data from CSV files into Bronze layer tables
           Bronze layer stores raw data from source systems (CRM & ERP)
  
  AUTHOR: Data Engineering Team
  CREATED: 2025
  
  TABLES AFFECTED:
    - bronze.crm_cust_info
    - bronze.crm_prd_info
    - bronze.crm_sales_details
    - bronze.erp_cust_az12
    - bronze.erp_loc_a101
    - bronze.erp_px_cat_g1v2
  
  PREREQUISITES:
    1. All bronze schema tables must exist
    2. CSV files must be accessible at specified file paths
    3. SQL Server service account must have read permissions on file paths
    4. Database must have BULK INSERT permissions enabled
  
  EXECUTION: EXEC bronze.load_bronze
  
  WARNING: This procedure TRUNCATES all data before loading!
           All existing data in bronze tables will be lost.
==============================================================================*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
    -- ========================================================================
    -- VARIABLE DECLARATION
    -- ========================================================================
    DECLARE @Start_time DATETIME,      -- Stores start time for duration tracking
            @End_time DATETIME,        -- Stores end time for duration calculation
            @Overall_Start DATETIME;   -- Stores overall procedure start time

    BEGIN TRY 
        -- ====================================================================
        -- PROCEDURE START
        -- ====================================================================
        SET @Overall_Start = GETDATE();  -- Capture overall start time
        
        PRINT '====================================================================';
        PRINT 'Loading Bronze Layer';
        PRINT 'Started at: ' + CONVERT(VARCHAR, @Overall_Start, 120);
        PRINT '====================================================================';

        -- ====================================================================
        -- SECTION 1: CRM DATA SOURCE TABLES
        -- Source System: Customer Relationship Management (CRM)
        -- ====================================================================
        PRINT '--------------------------------------------';
        PRINT 'Loading CRM Data Source Tables';
        PRINT '--------------------------------------------';

        -- ==================== TABLE 1: CRM Customer Info ====================
        -- Description: Customer master data from CRM system
        -- Source File: cust_info.csv
        -- WARNING: TRUNCATE will delete all existing records
        -- ====================================================================
        SET @Start_time = GETDATE();
        PRINT '>>> Truncating Table bronze.crm_cust_info';
        
        TRUNCATE TABLE bronze.crm_cust_info;  -- ⚠️ WARNING: Deletes all data!
        
        PRINT '>>> Inserting Data Into Table bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\Mega Computer\Downloads\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,              -- Skip header row
            FIELDTERMINATOR = ',',     -- CSV comma delimiter
            TABLOCK                    -- Table-level lock for performance
        );
        
        SET @End_time = GETDATE();
        PRINT ' >>> Load Duration: ' + CAST(DATEDIFF(SECOND, @Start_time, @End_time) AS NVARCHAR(50)) + ' Seconds';
        PRINT ' >>> Records Loaded: ' + CAST(@@ROWCOUNT AS NVARCHAR(50));  -- Show row count
        PRINT ' >> ----------------';

        -- ==================== TABLE 2: CRM Product Info ====================
        -- Description: Product master data from CRM system
        -- Source File: prd_info.csv
        -- WARNING: TRUNCATE will delete all existing records
        -- ====================================================================
        SET @Start_time = GETDATE();
        PRINT '>>> Truncating Table bronze.crm_prd_info';
        
        TRUNCATE TABLE bronze.crm_prd_info;  -- ⚠️ WARNING: Deletes all data!
        
        PRINT '>>> Inserting Data Into Table bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\Mega Computer\Downloads\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,              -- Skip header row
            FIELDTERMINATOR = ',',     -- CSV comma delimiter
            TABLOCK                    -- Table-level lock for performance
        );
        
        SET @End_time = GETDATE();
        PRINT ' >>> Load Duration: ' + CAST(DATEDIFF(SECOND, @Start_time, @End_time) AS NVARCHAR(50)) + ' Seconds';
        PRINT ' >>> Records Loaded: ' + CAST(@@ROWCOUNT AS NVARCHAR(50));  -- Show row count
        PRINT ' >> ----------------';

        -- ==================== TABLE 3: CRM Sales Details ====================
        -- Description: Sales transaction details from CRM system
        -- Source File: sales_details.csv
        -- WARNING: TRUNCATE will delete all existing records
        -- NOTE: This is typically the largest table
        -- ====================================================================
        SET @Start_time = GETDATE();
        PRINT '>>> Truncating Table bronze.crm_sales_details';
        
        TRUNCATE TABLE bronze.crm_sales_details;  -- ⚠️ WARNING: Deletes all data!
        
        PRINT '>>> Inserting Data Into Table bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\Mega Computer\Downloads\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,              -- Skip header row
            FIELDTERMINATOR = ',',     -- CSV comma delimiter
            TABLOCK                    -- Table-level lock for performance
        );
        
        SET @End_time = GETDATE();
        PRINT ' >>> Load Duration: ' + CAST(DATEDIFF(SECOND, @Start_time, @End_time) AS NVARCHAR(50)) + ' Seconds';
        PRINT ' >>> Records Loaded: ' + CAST(@@ROWCOUNT AS NVARCHAR(50));  -- Show row count
        PRINT ' >> ----------------';

        -- ====================================================================
        -- SECTION 2: ERP DATA SOURCE TABLES
        -- Source System: Enterprise Resource Planning (ERP)
        -- ====================================================================
        PRINT '--------------------------------------------';
        PRINT 'Loading ERP Data Source Tables';
        PRINT '--------------------------------------------';

        -- ==================== TABLE 4: ERP Customer Data ====================
        -- Description: Customer data from ERP system (code: az12)
        -- Source File: cust_az12.csv
        -- WARNING: TRUNCATE will delete all existing records
        -- ====================================================================
        SET @Start_time = GETDATE();
        PRINT '>>> Truncating Table bronze.erp_cust_az12';
        
        TRUNCATE TABLE bronze.erp_cust_az12;  -- ⚠️ WARNING: Deletes all data!
        
        PRINT '>>> Inserting Data Into Table bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\Mega Computer\Downloads\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
        WITH (
            FIRSTROW = 2,              -- Skip header row
            FIELDTERMINATOR = ',',     -- CSV comma delimiter
            TABLOCK                    -- Table-level lock for performance
        );
        
        SET @End_time = GETDATE();
        PRINT ' >>> Load Duration: ' + CAST(DATEDIFF(SECOND, @Start_time, @End_time) AS NVARCHAR(50)) + ' Seconds';
        PRINT ' >>> Records Loaded: ' + CAST(@@ROWCOUNT AS NVARCHAR(50));  -- Show row count
        PRINT ' >> ----------------';

        -- ==================== TABLE 5: ERP Location Data ====================
        -- Description: Location/geography data from ERP system (code: a101)
        -- Source File: loc_a101.csv
        -- WARNING: TRUNCATE will delete all existing records
        -- ====================================================================
        SET @Start_time = GETDATE();
        PRINT '>>> Truncating Table bronze.erp_loc_a101';
        
        TRUNCATE TABLE bronze.erp_loc_a101;  -- ⚠️ WARNING: Deletes all data!
        
        PRINT '>>> Inserting Data Into Table bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\Mega Computer\Downloads\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
        WITH (
            FIRSTROW = 2,              -- Skip header row
            FIELDTERMINATOR = ',',     -- CSV comma delimiter
            TABLOCK                    -- Table-level lock for performance
        );
        
        SET @End_time = GETDATE();
        PRINT ' >>> Load Duration: ' + CAST(DATEDIFF(SECOND, @Start_time, @End_time) AS NVARCHAR(50)) + ' Seconds';
        PRINT ' >>> Records Loaded: ' + CAST(@@ROWCOUNT AS NVARCHAR(50));  -- Show row count
        PRINT ' >> ----------------';

        -- ==================== TABLE 6: ERP Product Category ================
        -- Description: Product category data from ERP system (code: g1v2)
        -- Source File: px_cat_g1v2.csv
        -- WARNING: TRUNCATE will delete all existing records
        -- ====================================================================
        SET @Start_time = GETDATE();
        PRINT '>>> Truncating Table bronze.erp_px_cat_g1v2';
        
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;  -- ⚠️ WARNING: Deletes all data!
        
        PRINT '>>> Inserting Data Into Table bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\Mega Computer\Downloads\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
        WITH (
            FIRSTROW = 2,              -- Skip header row
            FIELDTERMINATOR = ',',     -- CSV comma delimiter
            TABLOCK                    -- Table-level lock for performance
        );
        
        SET @End_time = GETDATE();
        PRINT ' >>> Load Duration: ' + CAST(DATEDIFF(SECOND, @Start_time, @End_time) AS NVARCHAR(50)) + ' Seconds';
        PRINT ' >>> Records Loaded: ' + CAST(@@ROWCOUNT AS NVARCHAR(50));  -- Show row count
        PRINT ' >> ----------------';

        -- ====================================================================
        -- SUCCESS: ALL TABLES LOADED SUCCESSFULLY
        -- ====================================================================
        PRINT '====================================================================';
        PRINT 'Bronze Layer Load Completed Successfully!';
        PRINT 'Completed at: ' + CONVERT(VARCHAR, GETDATE(), 120);
        PRINT 'Total Duration: ' + CAST(DATEDIFF(SECOND, @Overall_Start, GETDATE()) AS NVARCHAR(50)) + ' Seconds';
        PRINT '====================================================================';

    END TRY 
    
    -- ========================================================================
    -- ERROR HANDLING BLOCK
    -- Captures and logs any errors that occur during execution
    -- ========================================================================
    BEGIN CATCH 
        PRINT '==================================================';
        PRINT '❌ ERROR OCCURRED DURING LOADING BRONZE LAYER';
        PRINT '==================================================';
        PRINT 'ERROR MESSAGE: ' + ERROR_MESSAGE();
        PRINT 'ERROR NUMBER: ' + CAST(ERROR_NUMBER() AS NVARCHAR(50));
        PRINT 'ERROR SEVERITY: ' + CAST(ERROR_SEVERITY() AS NVARCHAR(50));
        PRINT 'ERROR STATE: ' + CAST(ERROR_STATE() AS NVARCHAR(50));
        PRINT 'ERROR LINE: ' + CAST(ERROR_LINE() AS NVARCHAR(10));
        PRINT 'ERROR PROCEDURE: ' + ISNULL(ERROR_PROCEDURE(), 'N/A');
        PRINT '==================================================';
        PRINT '⚠️  WARNING: Some or all tables may be empty due to error!';
        PRINT '    Recommended Action: Review error and re-run procedure';
        PRINT '==================================================';
        
        -- NOTE: Consider adding ROLLBACK TRANSACTION if using transactions
        -- ROLLBACK TRANSACTION;
        
        -- RE-THROW error to calling application (optional)
        -- THROW;
    END CATCH 
END

/*==============================================================================
  USAGE NOTES:
  
  1. BULK INSERT Requirements:
     - SQL Server service account needs READ permission on file paths
     - Files must be accessible from SQL Server (not client machine)
     - Use UNC paths for network locations: \\server\share\file.csv
  
  2. Performance Considerations:
     - TABLOCK option improves performance but locks entire table
     - Consider running during off-peak hours for large datasets
     - Monitor transaction log space during execution
  
  3. Error Recovery:
     - If procedure fails, tables will be in TRUNCATED state (empty)
     - Always have backup or source files available for re-run
     - Consider implementing checkpoint/restart logic for large loads
  
  4. Monitoring:
     - Review printed output for duration of each table load
     - Check @@ROWCOUNT to verify expected number of records loaded
     - Monitor SQL Server error logs for any warnings
  
  5. Maintenance:
     - Update file paths if source locations change
     - Adjust FIELDTERMINATOR if CSV format changes
     - Add/remove tables as source systems evolve
==============================================================================*/
