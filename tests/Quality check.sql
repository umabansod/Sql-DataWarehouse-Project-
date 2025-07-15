/*
*******************************************************************
	Quality Checks
*******************************************************************
Purpose:
	This scripts performs various quality checks for data consistency,
accuracy, and standardization across the 'silver' schemas. It includes checks 
for :
	- Null or duplicate primary keys.
	- Unwanted spaces in string fields.
	- Data standardization and consistency.
	- Invalid date ranges and orders.
	- Data consistency between related fields.

Run these checks after data loading silver layer.
Investigate and resolve any discrepancies found during the checks.
*******************************************************************
*/




select * from bronze.crm_cust_info;
select * from silver.crm_cust_info

**-- check for NULLS  and Duplicate Primary key **


select cst_id, count(*) from Silver.crm_cust_info
Group by cst_id
having count(*)>1 or cst_id is null ;

--- check for unwanted spaces for firstname and lastname 

select cst_lastname 
from Silver.crm_cust_info
where cst_lastname  != Trim(cst_lastname );


---- Data Standardization and Consistency for marital status, gndr

select distinct cst_marital_status
from silver.crm_cust_info

*************************************************************
		--	 CRM PRODUCT TABLE
*************************************************************

----- Check missing value from prd_id and duplicate primary key 

select prd_id, count(*) 
from bronze.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id IS NULL;

select * from bronze.crm_prd_info;
SELECT * FROM bronze.crm_sales_details;
select * from bronze.erp_px_cat_g1v2;

select distinct(id) from bronze.erp_px_cat_g1v2
SELECT * FROM BRONZE.crm_sales_details



 select * from bronze.crm_prd_info;
----- extract specific part of string 
select prd_id,
prd_key,
replace(substring(prd_key,1,5), '-','_')as cat_id,
SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost,0)AS prd_cost,
prd_line,
cast( prd_start_dt as date),
cast (LEAD (prd_start_dt) over ( partition by prd_Key order by prd_start_dt)-1 as Date) as prd_end_dt
from bronze.crm_prd_info

-----
-- where SUBSTRING(prd_key,7,LEN(prd_key)) not in 
--( select  sls_prd_key from bronze.crm_sales_details) where sls_prd_key like 'FK%');
--where replace(substring(prd_key,1,5), '-','_') NOT IN ( SELECT DISTINCT ID FROM BRONZE.erp_px_cat_g1v2)***


---- check for unwanted spaces 
select prd_id from Silver.crm_prd_info
where prd_nm != trim ( prd_nm)


---- Check negative or nulls from cost 
select prd_cost from Silver.crm_prd_info where
prd_cost <=0 or prd_cost is null;

----- Data Standardization and consistency 
select distinct prd_line from Silver.crm_prd_info;


----- Check for Invalid Date orders 

select prd_key,prd_start_dt,
prd_end_dt 
from Silver.crm_prd_info
where prd_start_dt> prd_end_dt;


***********************************
		--CRM Sales Table
***********************************

SELECT 
sls_ord_num,
sls_prd_Key,
sls_cust_id,
case when sls_order_dt = 0 or len (sls_order_dt) != 8 then NULL
	ELSE CAST(CAST(sls_order_dt as varchar ) as date )
end as sls_order_dt, 
case when sls_ship_dt = 0 or len (sls_ship_dt) != 8 then NULL
	ELSE CAST(CAST(sls_ship_dt as varchar ) as date )
end as sls_ship_dt,
case when sls_due_dt = 0 or len (sls_due_dt) != 8 then NULL
	ELSE CAST(CAST(sls_due_dt as varchar ) as date )
end as sls_due_dt,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
	THEN  sls_quantity * abs ( sls_price )	
ELSE sls_sales 
END as sls_sales ,
sls_quantity,
CASE WHEN sls_price  IS NULL OR sls_price <=0 
	THEN sls_sales / NULLIF(sls_quantity ,0 )
	ELSE sls_price
END as sls_price
FROM bronze.crm_sales_details 


select * from bronze.crm_sales_details ;

-- check order is duplicate or null

select sls_ord_num, count(*) 
from bronze.crm_sales_details
group by sls_ord_num
having count(*) > 1 or sls_ord_num IS NULL; 


---- check for invalid date

** - sls_order_dt - **
select
NULLIF(sls_order_dt,0) sls_order_dt
from silver.crm_sales_details
where sls_order_dt <= 0 
or len(sls_order_dt) != 8  
or sls_order_dt > 20500101
or sls_order_dt < 19000101;

** - sls_ship_dt - **

select 
NULLIF(sls_ship_dt,0) sls_ship_dt
from silver.crm_sales_details
where sls_ship_dt <= 0 
or len(sls_ship_dt) != 8  
or sls_ship_dt > 20500101
or sls_ship_dt < 19000101;


 --- Check for invalid date order s

 select 
 * 
 from silver.crm_sales_details
 where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt 


--- check data consistency : Between sales, Quantity, and Price
--- >> Sales = price * quantity 
--- >> value must not be null, zero or negative 

select 
sls_sales,
sls_quantity,
sls_price as old_price,

CASE WHEN sls_sales IS NULL OR sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
	THEN  sls_quantity * abs ( sls_price )	
ELSE sls_sales 
END as sls_sales ,
sls_quantity,
CASE WHEN sls_price  IS NULL OR sls_price <=0 
	THEN sls_sales / NULLIF(sls_quantity ,0 )
	ELSE sls_price
END as sls_price
from silver.crm_sales_details
where sls_sales != sls_quantity * sls_price 
or sls_sales  IS NULL OR sls_quantity IS NULL OR sls_price IS NULL 
OR sls_sales <= 0  or sls_quantity <= 0 or sls_price <= 0 

--Solution  1) Data Issues will be fixed direct in source system 
--			2) Data isssues has to be fixed in data warehouse 
--			3) If sales is negative, zero or null derive it using quantity and price 
--			4) if price is zero or null, calculate it using sales and quantity
--			5) if price is negative , convert it to a positive value
 

  ******************************************
 -- ERP CUSTOMER TABLE 
 ******************************************
 SELECT * FROM bronze.erp_cust_az12;
 SELECT * FROM bronze.crm_cust_info;
 SELECT * FROM silver.crm_cust_info;


 -- check wheather erp is subset of crm 
 SELECT 
 CASE WHEN cid like 'NAS%' THEN substring(cid, 4, len(cid))
 ELSE cid
 end as cid,
 bdate,
 gen 
 from silver.erp_cust_az12
 where CASE WHEN cid like 'NAS%' THEN substring(cid, 4, len(cid))
 ELSE cid
 end NOT IN ( SELECT DISTINCT cst_key from silver.crm_cust_info);


 -- identify out-of range dates

 select distinct 
 bdate 
 from 
silver.erp_cust_az12;
 --(SELECT 
 CASE WHEN cid like 'NAS%' THEN substring(cid, 4, len(cid))
 ELSE cid
 end as cid,
 case when bdate > GETDATE() THEN NULL
 ELSE bdate
 end AS bdate,
 gen 
 FROM bronze.erp_cust_az12)t
 where bdate < '1924-01-01'OR bdate > GETDATE()
 --;


 -- Data standardization & consitency 
SELECT DISTINCT gen 
from silver.erp_cust_az12;



*********************************************
		-- ERP LOC_a101
*********************************************


select * from bronze.erp_loc_a101;
select * from bronze.crm_cust_info;


SELECT 
REPLACE ( cid, '-','') cid,
cntry
from bronze.erp_loc_a101
where REPLACE ( cid, '-','') NOT IN (SELECT cst_key from silver.crm_cust_info)
;


-- Data Standardization and consistency 
select distinct cntry from silver.erp_loc_a101


select 
cid,
cntry,
case when TRIM(cntry) = 'DE' THEN  'Germany'
	when TRIM(cntry) IN ('US','USA') THEN 'United States'
	WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
ELSE TRIM(cntry)
end as cntry
from silver.erp_loc_a101;


**********************************
-- ERP PX CAT 
**********************************


SELECT * FROM silver.crm_prd_info;
select * from bronze.erp_px_cat_g1v2;
------ Check unwanted spaces 
select 
id,
cat,
subcat,
maintenance
from bronze.erp_px_cat_g1v2
where cat != trim(cat) or subcat != trim ( subcat ) or maintenance != trim(maintenance)


---- Data Standardization & consistency 

select distinct subcat 
from bronze.erp_px_cat_g1v2;
