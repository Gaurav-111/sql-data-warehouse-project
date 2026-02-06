/*
    this script define then ddl for silver layer
  
*/
CREATE TABLE silver.crm_cust_info(
    cst_id int,
	cst_key varchar(50),
	cst_firstname varchar(50),
	cst_lastname varchar(50),
	cst_material_status varchar(50),
	cst_gndr varchar(50),
	cst_create_date date,
	dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP

);

CREATE TABLE silver.crm_prd_info(
   prd_id int,
   prd_key varchar(50),
   prd_nm varchar(50),
   prd_cost int,
   prd_line varchar(50),
   prd_start_dt date,
   prd_end_dt date,
   dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP


);

CREATE TABLE silver.crm_sales_details(
   sls_ord_num varchar(50),
   sls_prd_key varchar(50),
   sls_cust_id int,
   sls_order_dt int,
   sls_ship_dt int,
   sls_due_dt int,
   sls_sales int,
   sls_quality int,
   sls_price int,
   dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP

);
CREATE TABLE silver.erp_loc_a101(
   cid varchar(50),
   cntry varchar(50),
   dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP

);
CREATE TABLE silver.erp_cust_az12(
  cid varchar(50),
  bdate date,
  gen varchar(50),
  dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP

);
CREATE TABLE silver.erp_px_cat_g1v2 (
   id varchar(50),
   cat varchar(50),
   subcat varchar(50),
   maintenance varchar(50),
   dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
