/*
   gold_layer(star_schema)
   purpose of script -> This script create a view called fact_sales in gold layer ,
   it is a fact table of star_schema ,this view contains all measurable or quantity information about customers and products
  
*/
create view gold.fact_sales as 
SELECT 
sd.sls_ord_num as order_number,
pr.product_key ,
cu.customer_key,
sd.sls_order_dt as order_date,
sd.sls_ship_dt as shipping_date, 
sd.sls_due_dt as due_date,
sd.sls_sales as sales_amount,
sd.sls_quantity as quantity,
sd.sls_price as price
FROM silver.crm_sales_details as sd
left join gold.dim_products as pr
on sd.sls_prd_key = pr.product_number
left join gold.dim_customers as cu
on sd.sls_cust_id = cu.customer_id
