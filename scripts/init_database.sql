/*
=================================================================================
Create DATABASE and SCHEMAS
=================================================================================
Script Purpose:
This script creates a new database 'Datawarehouse' after checking if it already exists.
If the database already exists, it is dropped and recreated. Additionally, the script sets
up 3 schemas named 'Bronze','silver' & 'gold' within the database.

Warning:
Running this script will drop the entire 'Datawarehouse' Database if it exists.
All data in the database will be permamnetly deleted. Proceed with caution and 
ensure you have proper backups before running this script.
*/





Use master;
GO

-- Drop and Recreate the 'Datawarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name='Datawarehouse')
BEGIN
    ALTER DATABASE Datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE Datawarehouse;
END;
GO

--Create the 'Datawarehouse' database
CREATE DATABASE Datawarehouse;
GO

USE Datawarehouse;
GO

--Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO 
