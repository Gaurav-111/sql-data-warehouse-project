/* 
   gold_layer(star_schema)
   purpose of script -> This script create a view called dim_customers in gold layer
   this view contains all descriptive  information about customers , it have a surrogate key(customer_key) to connect with fact_sales view
*/
create view gold.dim_customers as
select 
row_number() over(order by cst_id) as customer_key,
ci.cst_id as customer_id,
ci.cst_key as customer_number,
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
la.cntry as country,
ci.cst_marital_status as marital_status,
case
   when ci.cst_gndr != 'n/a' then ci.cst_gndr
   else coalesce(ca.gen , 'n/a')
end as gender ,
ca.bdate as birthdate,
ci.cst_create_date as create_date
from silver.crm_cust_info as ci
left join silver.erp_cust_az12 as ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 as la
on ci.cst_key = la.cid
