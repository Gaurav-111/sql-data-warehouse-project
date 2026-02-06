/*
    stored procedure - load_silver (bronze -> silver)
    script purpose - This script create a procedure that is load data from bronze layer to silver layer,
    first this truncate the table and then load data into tables
*/
create or replace procedure silver.load_silver()
language plpgsql
as $$
begin
		RAISE NOTICE '====================================================================================';
		RAISE NOTICE 'loading silver layer';
		RAISE NOTICE '====================================================================================';
		RAISE NOTICE 'Truncate and loading silver.crm_cust_info 1';
		RAISE NOTICE '-------------------------------------------------------------------';
		truncate table silver.crm_cust_info;
		insert into silver.crm_cust_info(
		  cst_id,
		  cst_key,
		  cst_firstname,
		  cst_lastname,
		  cst_marital_status,
		  cst_gndr,
		  cst_create_date
		)
		select 
		cst_id,
		cst_key,
		trim(cst_firstname) as cst_firstname,
		trim(cst_lastname) as cst_lastname,
		case 
		   when upper(trim(cst_material_status)) = 'S' then 'Single'
		   when upper(trim(cst_material_status)) = 'M' then 'Married'
		   else 'n/a'
		end as cst_marital_status,
		case 
		   when upper(trim(cst_gndr)) = 'F' then 'Female'
		   when upper(trim(cst_gndr)) = 'M' then 'Male'
		   else 'n/a'
		end as cst_gndr,
		cst_create_date
		from (
				select *,
				row_number() over(partition by cst_id order by cst_create_date desc) as flag_last
				from bronze.crm_cust_info
				where cst_id is not null
		)t
		where flag_last = 1;

				/*   
		    first modify the silver.crm_prd_info ddl before insert data   
			because we derive two new columns and some modification to data
		   
		drop table silver.crm_prd_info
		create table silver.crm_prd_info(
		   prd_id int,
		   cat_id varchar(50),
		   prd_key varchar(50),
		   prd_nm varchar(50),
		   prd_cost int,
		   prd_line varchar(50),
		   prd_star_dt date,
		   prd_end_dt date,
		   dwh_create_date timestamp default current_timestamp
		);
		*/
		RAISE NOTICE '====================================================================================';
		RAISE NOTICE 'Truncate and loading silver.crm_prd_info 2';
		RAISE NOTICE '-------------------------------------------------------------------';
		truncate table silver.crm_prd_info;
		insert into silver.crm_prd_info(
		   prd_id,
		   cat_id,
		   prd_key,
		   prd_nm,
		   prd_cost,
		   prd_line,
		   prd_star_dt,
		   prd_end_dt 
		)
		select 
		prd_id,
		replace(substring(prd_key , 1,5),'-','_') as cat_id,
		substring(prd_key , 7 , length(prd_key)) as prd_key,
		prd_nm,
		coalesce(prd_cost, 0) as prd_cost,
		
		case
		  when upper(trim(prd_line)) = 'M' then 'Mountain'
		  when upper(trim(prd_line)) = 'R' then 'Road'
		  when upper(trim(prd_line)) = 'S' then 'Other Sales'
		  when upper(trim(prd_line)) = 'T' then 'Touring'
		  else 'n/a'
		end as prd_line,
		cast(prd_start_dt as date) as prd_start_dt,
		cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)- interval '1 day' as date) as prd_end_dt
		
		from bronze.crm_prd_info;


				/*
		    change ddl for silver.crm_sales_details
			because we change data-type integer to date for order_dt , ship_dt,_due_dt columns
		  
		drop table silver.crm_sales_details
		CREATE TABLE silver.crm_sales_details(
		   sls_ord_num varchar(50),
		   sls_prd_key varchar(50),
		   sls_cust_id int,
		   sls_order_dt date,
		   sls_ship_dt date,
		   sls_due_dt date,
		   sls_sales int,
		   sls_quantity int,
		   sls_price int,
		   dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		
		);
		*/
		RAISE NOTICE '====================================================================================';
		RAISE NOTICE 'Truncate and loading silver.crm_sales_details 3';
		RAISE NOTICE '-------------------------------------------------------------------';
		truncate table silver.crm_sales_details;
		insert into silver.crm_sales_details(
		   sls_ord_num,
		   sls_prd_key,
		   sls_cust_id,
		   sls_order_dt,
		   sls_ship_dt,
		   sls_due_dt,
		   sls_sales,
		   sls_quantity,
		   sls_price
		)
		SELECT 
		sls_ord_num, 
		sls_prd_key, 
		sls_cust_id,
		case 
		   when sls_order_dt = 0 or length(sls_order_dt::text) != 8 then null
		   else to_date(sls_order_dt::text , 'YYYYMMDD')
		end as sls_order_dt,
		case 
		   when sls_ship_dt = 0 or length(sls_ship_dt::text) != 8 then null
		   else to_date(sls_ship_dt::text , 'YYYYMMDD')
		end as sls_ship_dt,
		case 
		   when sls_due_dt = 0 or length(sls_due_dt::text) != 8 then null
		   else to_date(sls_due_dt::text , 'YYYYMMDD')
		end as sls_due_dt,
		case 
		   when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * abs(sls_price) then sls_quantity * abs(sls_price)
		   else sls_sales
		end as sls_sales,
		sls_quantity, 
		case
		  when sls_price is null or sls_price <=0 then sls_sales / nullif(sls_quantity , 0)
		  else sls_price
		end as sls_price
		FROM bronze.crm_sales_details;

		RAISE NOTICE '====================================================================================';
		RAISE NOTICE 'Truncate and loading silver.erp_cust_az12 4';
		RAISE NOTICE '-------------------------------------------------------------------';	
		truncate table silver.erp_cust_az12;
		insert into silver.erp_cust_az12(
		cid,
		bdate,
		gen
		)
		SELECT 
		case
		  when cid like 'NAS%' then substring(cid , 4 , length(cid))
		  else cid
		end as cid,
		case
		   when bdate > current_date then null
		   else bdate
		end as bdate,
		case 
		   when upper(trim(gen)) in ('F','FEMALE') then 'Female'
		   when upper(trim(gen)) in ('M','MALE') then 'Male'
		   else 'n/a'
		end as gen
		FROM bronze.erp_cust_az12;

		RAISE NOTICE '====================================================================================';
		RAISE NOTICE 'Truncate and loading silver.erp_loc_a101 5';
		RAISE NOTICE '-------------------------------------------------------------------';		
		truncate table silver.erp_loc_a101;
		insert into silver.erp_loc_a101(
		  cid,
		  cntry
		)
		SELECT 
		replace(cid, '-' , '') as cid,
		case
		  when trim(cntry) = 'DE' then 'Germany'
		  when trim(cntry) in ('US','USA') then 'United States'
		  when trim(cntry) = '' or cntry is null then 'n/a'
		  else trim(cntry)
		end as cntry
		FROM bronze.erp_loc_a101;
		RAISE NOTICE '====================================================================================';
		RAISE NOTICE 'Truncate and loading silver.erp_px_cat_g1v2 6';
		RAISE NOTICE '-------------------------------------------------------------------';		
		truncate table silver.erp_px_cat_g1v2;
		insert into silver.erp_px_cat_g1v2(
		   id,
		   cat,
		   subcat,
		   maintenance
		)
		SELECT 
		id,
		cat, 
		subcat,
		maintenance
		FROM bronze.erp_px_cat_g1v2;
		
				

end;
$$;
