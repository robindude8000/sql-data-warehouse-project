/*
===== Create Database and Schema =====
Script Purpose:
  Check 'DataWarehouse' database if exists, if yes, delete the database and proceed on creating the 'DataWarehouse' db.
  Then create 3 schemas within the database (bronze, silver, gold).

WARNING:
  Running this script will delete database along with data in it. 
  Proceed with caution and ensure to have proper backups before running the script.

*/

-- Check if 'DataWarehouse' db exists
USE master;
GO

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse
END;
GO

-- Create db 'DataWarehouse'
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create schemas (something like folder/layer)
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
