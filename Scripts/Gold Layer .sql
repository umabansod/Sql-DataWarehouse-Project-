/* 
****************************************************
DDL SCRIPT : CREATE GOLD VIEWS
****************************************************
SCRIPT PURPOSE :
	This Script creates for the gold layer in the data warehouse.
	The Gold layer represents the final dimension and fact tables (star schema)

	Each view performs transformation and combines data from the silver.layer
	to produce  a clean, enriched and business ready dataset.

Usage :
	 These views can be queried direclty for analytics and reporting .
*****************************************************
*/

******************************************
 -- Dimention Customer Table 
******************************************

CREATE VIEW gold.dim_customers As 
select 
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	ci.cst_marital_status AS marital_status ,
	case when ci.cst_gndr != 'n/a' then ci.cst_gndr
		else coalesce(ca.gen,'n/a')
	end as gender,
	ci.cst_create_date As create_date,
	ca.bdate as birthdate ,
	lo.cntry as country 
	from silver.crm_cust_info ci
	Left join  silver.erp_cust_az12 ca
	on ci.cst_key = ca.cid
	Left join silver.erp_loc_a101 lo
	on ci.cst_key = lo.cid


	select * from gold.dim_customers;

**********************************************
-- Dimension Product Table 
**********************************************

	select * from silver.crm_prd_info;
	select * from silver.erp_px_cat_g1v2;

Create view gold.dim_product as 
	select 
	ROW_NUMBER() OVER(ORDER BY pc.prd_start_dt, pc.prd_key) as product_key,
	pc.prd_id as product_id,
	pc.prd_key as product_number,
	pc.prd_nm as product_name,
	pc.cat_id as category_id,
	pn.cat as category,
	pn.subcat as subcategory,
	pn.maintenance as maintenance,
	pc.prd_cost as cost,
	pc.prd_line as product_line,
	pc.prd_start_dt as start_date
	from silver.crm_prd_info pc
	left join silver.erp_px_cat_g1v2 pn
	on pc.cat_id = pn.id


	select * from gold.dim_product


	*******************************************
	-- Fact Sales Table
	*******************************************

select * from silver.crm_sales_details;
select * from gold.dim_product;
select * from gold.dim_customers;

CREATE VIEW gold.fact_sales  AS
	select 
		sd.sls_ord_num  as order_number,
		pd.product_key,
		cu.customer_key ,
		sd.sls_order_dt as order_date,
		sd.sls_ship_dt as shipping_date,
		sd.sls_due_dt as due_date,
		sd.sls_sales as sales_amount,
		sd.sls_quantity as quantity,
		sd.sls_price as price
		from silver.crm_sales_details sd 
		left join gold.dim_customers cu
		on sd.sls_cust_id = cu.Customer_id
		left join gold.dim_product pd
		on sd.sls_prd_key = pd.product_number 


SELECT * FROM gold.fact_sales ;


-- FOREGIN KEY INTEGRITY 

select * 
from gold.fact_sales f
left join gold.dim_customers c
on c.customer_key = f.customer_key
left join gold.dim_product p
on p.product_key = f.product_key
where f.product_key IS NULL

