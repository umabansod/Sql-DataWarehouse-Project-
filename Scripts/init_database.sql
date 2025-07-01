/* 

***************************************
Create DAtabase and Schemas 
***************************************
*/


Use master;

--Created Database

Create Database DataWarehouse;

Use DataWarehouse;

CREATE SCHEMA bronze;
Go
CREATE SCHEMA Silver;
Go
CREATE SCHEMA gold;
Go
