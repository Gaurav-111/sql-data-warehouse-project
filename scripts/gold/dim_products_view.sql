/* 
   gold_layer(star_schema)
   purpose of script -> This script create a view called dim_products in gold layer
   this view contains all descriptive infromation about products and it have a key (product_key) to connection with fact_sales view
*/
create view gold.dim_products as
SELECT 
row_number() over(order by pn.prd_star_dt , pn.prd_key) as product_key,
pn.prd_id as product_id,
pn.prd_key as product_number,
pn.prd_nm as product_name,
pn.cat_id as category_id,
pc.cat as category,
pc.subcat as subcategory,
pc.maintenance,
pn.prd_cost as cost,
pn.prd_line as product_line, 
pn.prd_star_dt as start_date
FROM silver.crm_prd_info as pn
left join silver.erp_px_cat_g1v2 as pc
on pn.cat_id = pc.id
where pn.prd_end_dt is null ---fillter out all historical data , keep only present data
