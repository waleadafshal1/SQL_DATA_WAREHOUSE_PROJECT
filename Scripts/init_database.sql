/*
================================================================================
Database Setup Script - DataWarehouse
================================================================================
Purpose: Initialize the DataWarehouse database with a medallion architecture
Author: Waleed Javed 
Date: 05/10/2025
Description: This script creates a fresh DataWarehouse database with three schemas
            following the bronze-silver-gold data layering pattern
================================================================================
*/

-- Switch to master database to perform database-level operations
USE master;
GO

/*
--------------------------------------------------------------------------------
Step 1: Drop existing DataWarehouse database if it exists
--------------------------------------------------------------------------------
This ensures a clean slate by removing any previous version of the database.
The SINGLE_USER mode forcefully disconnects all active users before dropping.
--------------------------------------------------------------------------------
*/
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    -- Force disconnect all active connections to the database
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    
    -- Drop the existing database
    DROP DATABASE DataWarehouse;
END
GO

/*
--------------------------------------------------------------------------------
Step 2: Create new DataWarehouse database
--------------------------------------------------------------------------------
Creates a fresh DataWarehouse database for storing and processing data
--------------------------------------------------------------------------------
*/
CREATE DATABASE DataWarehouse;
GO

-- Switch context to the newly created database
USE DataWarehouse;
GO

/*
--------------------------------------------------------------------------------
Step 3: Create Schemas (Medallion Architecture)
--------------------------------------------------------------------------------
Implements a three-tier data quality architecture:
- BRONZE: Raw data layer (landing zone for source data)
- SILVER: Cleaned and validated data layer (processed data)
- GOLD: Business-level aggregated data layer (analytics-ready data)
--------------------------------------------------------------------------------
*/

-- Bronze Schema: Raw/Landing zone for unprocessed data from source systems
CREATE SCHEMA bronze;
GO

-- Silver Schema: Cleaned, validated, and conformed data
CREATE SCHEMA silver;
GO

-- Gold Schema: Business-level aggregated data ready for analytics and reporting
CREATE SCHEMA gold;
GO

/*
================================================================================
End of Database Setup Script
================================================================================
*/
