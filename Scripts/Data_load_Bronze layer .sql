/* 
*********************************************
Stored procedure : Load Bronze Layer 
*********************************************


/***** INSERTING DATA INTO TABLE *****/
-----------------------------------------

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @START_TIME DATETIME, @END_TIME DATETIME, @START_BATCH DATETIME, @END_BATCH DATETIME ;
	BEGIN TRY 
		SET @START_BATCH =GETDATE();
		PRINT ' ================================================='
		PRINT ' LOADING A BRONZE LAYER '
		PRINT ' ================================================='

		PRINT ' -------------------------------------------------'
		PRINT ' LOADING A CRM TABLE'
		PRINT ' -------------------------------------------------'

		SET @START_TIME= GETDATE();
		PRINT ' >> TRUNCATING TABLE: bronze.crm_cust_info'
		TRUNCATE TABLE  bronze.crm_cust_info;

		PRINT ' >> INSERTING DATA INTO : bronze.crm_cust_info'
		BULK INSERT bronze.crm_cust_info
		FROM  'E:\uma work\uma data analysis\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',' ,
			TABLOCK 
		);
		SET @END_TIME = GETDATE();
		PRINT ' >> LOAD_DURATION: ' + CAST(DATEDIFF(SECOND,@START_TIME, @END_TIME) AS NVARCHAR)+ ' SECONDS';

		SET @START_TIME= GETDATE();
		PRINT ' >> TRUNCATING TABLE: bronze.crm_prd_info'
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT ' >> INSERTING DATA INTO : bronze.crm_prd_info'
		BULK INSERT bronze.crm_prd_info
		FROM 'E:\uma work\uma data analysis\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR= ',',
			TABLOCK 
		);
		SET @END_TIME = GETDATE();
		PRINT ' >> LOAD_DURATION: ' + CAST(DATEDIFF(SECOND,@START_TIME, @END_TIME) AS NVARCHAR)+ ' SECONDS';


		SET @START_TIME= GETDATE();
		PRINT ' >> TRUNCATING TABLE: bronze.crm_sales_details'
		TRUNCATE TABLE bronze.crm_sales_details
		PRINT ' >> INSERTING DATA INTO : bronze.crm_sales_details'
		BULK INSERT bronze.crm_sales_details
		FROM 'E:\uma work\uma data analysis\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @END_TIME = GETDATE();
		PRINT ' >> LOAD_DURATION: ' + CAST(DATEDIFF(SECOND,@START_TIME, @END_TIME) AS NVARCHAR)+ ' SECONDS';

		PRINT ' -------------------------------------------------'
		PRINT ' LOADING A CRM TABLE'
		PRINT ' -------------------------------------------------'
		SET @START_TIME= GETDATE();
		PRINT ' >> TRUNCATING TABLE: bronze.erp_cust_az12'
		TRUNCATE TABLE bronze.erp_cust_az12
		PRINT ' >> INSERTING DATA INTO : bronze.erp_cust_az12'
		BULK INSERT bronze.erp_cust_az12
		FROM 'E:\uma work\uma data analysis\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK 
		);
		SET @END_TIME = GETDATE();
		PRINT ' >> LOAD_DURATION: ' + CAST(DATEDIFF(SECOND,@START_TIME, @END_TIME) AS NVARCHAR)+ ' SECONDS';

		SET @START_TIME= GETDATE();
		PRINT ' >> TRUNCATING TABLE: bronze.erp_px_cat_g1v2'
		TRUNCATE TABLE bronze.erp_px_cat_g1v2
		PRINT ' >> INSERTING DATA INTO : bronze.erp_px_cat_g1v2'
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'E:\uma work\uma data analysis\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH(
			FIRSTROW =2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @END_TIME = GETDATE();
		PRINT ' >> LOAD_DURATION: ' + CAST(DATEDIFF(SECOND,@START_TIME, @END_TIME) AS NVARCHAR)+ ' SECONDS';

		SET @START_TIME= GETDATE();
		PRINT ' >> TRUNCATING TABLE: bronze.erp_loc_a101'
		TRUNCATE TABLE bronze.erp_loc_a101
		PRINT ' >> INSERTING DATA INTO : bronze.erp_loc_a101'
		BULK INSERT bronze.erp_loc_a101
		FROM 'E:\uma work\uma data analysis\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH(
			FIRSTROW =2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @END_TIME = GETDATE();
		PRINT ' >> LOAD_DURATION: ' + CAST(DATEDIFF(SECOND,@START_TIME, @END_TIME) AS NVARCHAR)+ ' SECONDS';
		PRINT '>> ----------------------'

		SET @END_BATCH = GETDATE();
		PRINT ' ==============================================='
		PRINT ' LOADING BRONZE LAYER COMPLETED '
		PRINT ' TOTAL DURATION : ' + CAST ( DATEDIFF(SECOND, @START_BATCH, @END_BATCH) AS NVARCHAR) + ' SECONDS';
		PRINT ' ================================================'


	END TRY 
	BEGIN CATCH 
		PRINT ' ================================================'
		PRINT ' ERROR OCCURED DURING LOADING BRONZE LAYER '
		PRINT ' ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT ' ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT ' ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR );
		PRINT ' ================================================'
	END CATCH 
END


EXEC bronze.load_bronze;

