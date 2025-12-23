/*
Script Purpose:
     This script truncate(remove data from table) the table of bronze schema and then load a data from dataset in our local machine,
     this script also calculate the time taken to load each table in database,
     at the end of script we varify the tables is successfully loaded or not by counting the rows of each table
     
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time   TIMESTAMP;
    duration   INTERVAL;
BEGIN
    -- =========================
    -- LOAD CRM CUSTOMER
    -- =========================
    start_time := clock_timestamp();

    TRUNCATE TABLE bronze.crm_cust_info;
    COPY bronze.crm_cust_info
    FROM 'C:/pgdata/bronze_load/source_crm/cust_info.csv'
    DELIMITER ','
    CSV HEADER;

    end_time := clock_timestamp();
    duration := end_time - start_time;

    RAISE NOTICE 
    'crm_cust_info loaded in % seconds',
    EXTRACT(EPOCH FROM duration);

    -- =========================
    -- LOAD CRM PRODUCT
    -- =========================
    start_time := clock_timestamp();

    TRUNCATE TABLE bronze.crm_prd_info;
    COPY bronze.crm_prd_info
    FROM 'C:/pgdata/bronze_load/source_crm/prd_info.csv'
    DELIMITER ','
    CSV HEADER;

    end_time := clock_timestamp();
    duration := end_time - start_time;

    RAISE NOTICE 
    'crm_prd_info loaded in % seconds',
    EXTRACT(EPOCH FROM duration);

    -- =========================
    -- LOAD CRM SALES
    -- =========================
    start_time := clock_timestamp();

    TRUNCATE TABLE bronze.crm_sales_details;
    COPY bronze.crm_sales_details
    FROM 'C:/pgdata/bronze_load/source_crm/sales_details.csv'
    DELIMITER ','
    CSV HEADER;

    end_time := clock_timestamp();
    duration := end_time - start_time;

    RAISE NOTICE 
    'crm_sales_details loaded in % seconds',
    EXTRACT(EPOCH FROM duration);

    -- =========================
    -- LOAD ERP CUSTOMER
    -- =========================
    start_time := clock_timestamp();

    TRUNCATE TABLE bronze.erp_cust_az12;
    COPY bronze.erp_cust_az12
    FROM 'C:/pgdata/bronze_load/source_erp/CUST_AZ12.csv'
    DELIMITER ','
    CSV HEADER;

    end_time := clock_timestamp();
    duration := end_time - start_time;

    RAISE NOTICE 
    'erp_cust_az12 loaded in % seconds',
    EXTRACT(EPOCH FROM duration);

    -- =========================
    -- LOAD ERP LOCATION
    -- =========================
    start_time := clock_timestamp();

    TRUNCATE TABLE bronze.erp_loc_a101;
    COPY bronze.erp_loc_a101
    FROM 'C:/pgdata/bronze_load/source_erp/LOC_A101.csv'
    DELIMITER ','
    CSV HEADER;

    end_time := clock_timestamp();
    duration := end_time - start_time;

    RAISE NOTICE 
    'erp_loc_a101 loaded in % seconds',
    EXTRACT(EPOCH FROM duration);

    -- =========================
    -- LOAD ERP PRICE CATEGORY
    -- =========================
    start_time := clock_timestamp();

    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    COPY bronze.erp_px_cat_g1v2
    FROM 'C:/pgdata/bronze_load/source_erp/PX_CAT_G1V2.csv'
    DELIMITER ','
    CSV HEADER;

    end_time := clock_timestamp();
    duration := end_time - start_time;

    RAISE NOTICE 
    'erp_px_cat_g1v2 loaded in % seconds',
    EXTRACT(EPOCH FROM duration);
                                                
END;
$$;


call bronze.load_bronze()  

select count(*) from bronze.crm_cust_info
select count(*) from bronze.crm_prd_info
select count(*) from bronze.crm_sales_details
select count(*) from bronze.erp_cust_az12
select count(*) from bronze.erp_loc_a101
select count(*) from bronze.erp_px_cat_g1v2
