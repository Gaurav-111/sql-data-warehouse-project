/* 
   =================================================================================================
   stored procedure - load_bronze()
   =================================================================================================
   purpose of script -
                    this script create a procedure in bronze schema 
                    and the procedure load data from local csv files to bronze tables, procedure first
                    truncate table and then load data into table
*/
CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$

BEGIN
		RAISE NOTICE '=========================================================================';
		RAISE NOTICE 'Loading bronze layer';
		RAISE NOTICE '=========================================================================';
		RAISE NOTICE '-------------------------------------------';
		RAISE NOTICE 'Loading CRM tables';
		RAISE NOTICE '-------------------------------------------';
		
		---Truncate and load crm_cust_info from csv_file
		
		TRUNCATE TABLE bronze.crm_cust_info;
		COPY bronze.crm_cust_info
		FROM 'C:\pgdata\bronze_load\source_crm\cust_info.csv'
		DELIMITER ','
		CSV HEADER;
		
		---Truncate and load crm_prd_info from csv_file
		
		TRUNCATE TABLE bronze.crm_prd_info;
		COPY bronze.crm_prd_info
		FROM 'C:\pgdata\bronze_load\source_crm\prd_info.csv'
		DELIMITER ','
		CSV HEADER;
		
		
		---Truncate and load crm_sales_details from csv_file
		
		TRUNCATE TABLE bronze.crm_sales_details;
		COPY bronze.crm_sales_details
		FROM 'C:\pgdata\bronze_load\source_crm\sales_details.csv'
		DELIMITER ','
		CSV HEADER;
		
		RAISE NOTICE '-------------------------------------------';
		RAISE NOTICE 'Loading ERP tables';
		RAISE NOTICE '-------------------------------------------';
		---Truncate and load erp_cust_az12 from csv_file
		
		TRUNCATE TABLE bronze.erp_cust_az12;
		COPY bronze.erp_cust_az12
		FROM 'C:\pgdata\bronze_load\source_erp\CUST_AZ12.csv'
		DELIMITER ','
		CSV HEADER;
		
		
		---Truncate and load erp_loc_a101 from csv_file
		
		TRUNCATE TABLE bronze.erp_loc_a101;
		COPY bronze.erp_loc_a101
		FROM 'C:\pgdata\bronze_load\source_erp\LOC_A101.csv'
		DELIMITER ','
		CSV HEADER;
		
		
		---Truncate and load erp_px_cat_g1v2 from csv_file
		
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		COPY bronze.erp_px_cat_g1v2
		FROM 'C:\pgdata\bronze_load\source_erp\PX_CAT_G1V2.csv' 
		DELIMITER ','
		CSV HEADER;

END;
$$;

