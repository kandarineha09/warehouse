
-- crm_cust_info
DROP TABLE IF EXISTS silver_crm_cust_info;

CREATE TABLE silver_crm_cust_info (
    cst_id             INTEGER,
    cst_key            VARCHAR(50),
    cst_firstname      VARCHAR(50),
    cst_lastname       VARCHAR(50),
    cst_marital_status VARCHAR(50),
    cst_gndr           VARCHAR(50),
    cst_create_date    DATE,
    dwh_create_date    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- crm_prd_info
DROP TABLE IF EXISTS silver_crm_prd_info;

CREATE TABLE silver_crm_prd_info (
    prd_id          INTEGER,
    cat_id          VARCHAR(50),
    prd_key         VARCHAR(50),
    prd_nm          VARCHAR(50),
    prd_cost        INTEGER,
    prd_line        VARCHAR(50),
    prd_start_dt    DATE,
    prd_end_dt      DATE,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- crm_sales_details
DROP TABLE IF EXISTS silver_crm_sales_details;

CREATE TABLE silver_crm_sales_details (
    sls_ord_num     VARCHAR(50),
    sls_prd_key     VARCHAR(50),
    sls_cust_id     INTEGER,
    sls_order_dt    DATE,
    sls_ship_dt     DATE,
    sls_due_dt      DATE,
    sls_sales       INTEGER,
    sls_quantity    INTEGER,
    sls_price       INTEGER,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- erp_loc_a101
DROP TABLE IF EXISTS silver_erp_loc_a101;

CREATE TABLE silver_erp_loc_a101 (
    cid             VARCHAR(50),
    cntry           VARCHAR(50),
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- erp_cust_az12
DROP TABLE IF EXISTS silver_erp_cust_az12;

CREATE TABLE silver_erp_cust_az12 (
    cid             VARCHAR(50),
    bdate           DATE,
    gen             VARCHAR(50),
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- erp_px_cat_g1v2
DROP TABLE IF EXISTS silver_erp_px_cat_g1v2;

CREATE TABLE silver_erp_px_cat_g1v2 (
    id              VARCHAR(50),
    cat             VARCHAR(50),
    subcat          VARCHAR(50),
    maintenance     VARCHAR(50),
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);







-- crm_cust_info: Bronze → Silver
INSERT INTO silver_crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
SELECT 
  cst_id, 
  cst_key, 
  cst_firstname, 
  cst_lastname, 
  cst_marital_status, 
  cst_gndr,
  CURDATE()  -- assuming no create_date in bronze
FROM bronze_crm_cust_info;


-- crm_prd_info: Bronze → Silver
INSERT INTO silver_crm_prd_info (prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
SELECT 
  prd_id,
  NULL,  -- no cat_id in bronze; you can update later using px_cat table
  prd_key,
  prd_nm,
  prd_cost,
  prd_line,
  prd_start_dt,
  prd_end_dt
FROM bronze_crm_prd_info;


-- crm_sales_details: Bronze → Silver
INSERT INTO silver_crm_sales_details (sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
SELECT 
  sls_ord_num,
  sls_prd_key,
  sls_cust_id,
  sls_order_dt,
  sls_ship_dt,
  sls_due_dt,
  sls_sales,
  sls_quantity,
  sls_price
FROM bronze_crm_sales_details;


-- erp_loc_a101: Bronze → Silver
INSERT INTO silver_erp_loc_a101 (cid, cntry)
SELECT cid, cntry
FROM bronze_erp_loc_a101;


-- erp_cust_az12: Bronze → Silver
INSERT INTO silver_erp_cust_az12 (cid, bdate, gen)
SELECT cid, bdate, gen
FROM bronze_erp_cust_az12;


-- erp_px_cat_g1v2: Bronze → Silver
INSERT INTO silver_erp_px_cat_g1v2 (id, cat, subcat, maintenance)
SELECT id, cat, subcat, maintenance
FROM bronze_erp_px_cat_g1v2;


