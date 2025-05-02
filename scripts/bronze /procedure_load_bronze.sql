DELIMITER $$

-- Procedure to truncate crm_cust_info table
CREATE PROCEDURE truncate_bronze_crm_cust_info()
BEGIN
    TRUNCATE TABLE bronze_crm_cust_info;
END$$

-- Procedure to truncate crm_prd_info table
CREATE PROCEDURE truncate_bronze_crm_prd_info()
BEGIN
    TRUNCATE TABLE bronze_crm_prd_info;
END$$

-- Procedure to truncate crm_sales_details table
CREATE PROCEDURE truncate_bronze_crm_sales_details()
BEGIN
    TRUNCATE TABLE bronze_crm_sales_details;
END$$

-- Procedure to truncate erp_loc_a101 table
CREATE PROCEDURE truncate_bronze_erp_loc_a101()
BEGIN
    TRUNCATE TABLE bronze_erp_loc_a101;
END$$

-- Procedure to truncate erp_cust_az12 table
CREATE PROCEDURE truncate_bronze_erp_cust_az12()
BEGIN
    TRUNCATE TABLE bronze_erp_cust_az12;
END$$

-- Procedure to truncate erp_px_cat_g1v2 table
CREATE PROCEDURE truncate_bronze_erp_px_cat_g1v2()
BEGIN
    TRUNCATE TABLE bronze_erp_px_cat_g1v2;
END$$

DELIMITER ;





-- Call the truncate procedure for crm_cust_info table
CALL truncate_bronze_crm_cust_info();

-- Call the truncate procedure for crm_prd_info table
CALL truncate_bronze_crm_prd_info();

-- Call the truncate procedure for crm_sales_details table
CALL truncate_bronze_crm_sales_details();

-- Call the truncate procedure for erp_loc_a101 table
CALL truncate_bronze_erp_loc_a101();

-- Call the truncate procedure for erp_cust_az12 table
CALL truncate_bronze_erp_cust_az12();

-- Call the truncate procedure for erp_px_cat_g1v2 table
CALL truncate_bronze_erp_px_cat_g1v2();




ALTER TABLE bronze_crm_cust_info MODIFY COLUMN cst_id FLOAT NULL;

-- Load bronze_crm_cust_info
LOAD DATA INFILE 'cust_info_sanitized.csv'
INTO TABLE bronze_crm_cust_info
FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(@cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr)
SET cst_id = NULLIF(@cst_id, '');



ALTER TABLE bronze_crm_prd_info MODIFY prd_cost INT NULL;
ALTER TABLE bronze_crm_prd_info MODIFY prd_end_dt DATE NULL;



-- Load bronze_crm_prd_info
LOAD DATA INFILE 'prd_info.csv'
INTO TABLE bronze_crm_prd_info
FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY  '\r\n'
IGNORE 1 LINES
(@prd_id, @prd_key, @prd_nm, @prd_cost, @prd_line, @prd_start_dt, @prd_end_dt)
SET 
  prd_id = @prd_id,
  prd_key = @prd_key,
  prd_nm = @prd_nm,
  prd_cost = NULLIF(@prd_cost, ''),
  prd_line = @prd_line,
  prd_start_dt = NULLIF(@prd_start_dt, ''),
  prd_end_dt = NULLIF(@prd_end_dt, '');


ALTER TABLE bronze_crm_sales_details 
MODIFY sls_order_dt DATE,
MODIFY sls_ship_dt DATE,
MODIFY sls_due_dt DATE;

-- Load bronze_crm_sales_details
LOAD DATA INFILE 'sales_details_cleaned.csv'
INTO TABLE bronze_crm_sales_details
FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(@sls_ord_num, @sls_prd_key, @sls_cust_id, @sls_order_dt, @sls_ship_dt, @sls_due_dt,
 @sls_sales, @sls_quantity, @sls_price)
SET
  sls_ord_num = @sls_ord_num,
  sls_prd_key = @sls_prd_key,
  sls_cust_id = @sls_cust_id,
  sls_order_dt = CASE 
                   WHEN LENGTH(NULLIF(TRIM(@sls_order_dt), '')) = 8 
                   THEN STR_TO_DATE(TRIM(@sls_order_dt), '%Y%m%d') 
                   ELSE NULL 
                 END,
  sls_ship_dt = CASE 
                   WHEN LENGTH(NULLIF(TRIM(@sls_ship_dt), '')) = 8 
                   THEN STR_TO_DATE(TRIM(@sls_ship_dt), '%Y%m%d') 
                   ELSE NULL 
                 END,
  sls_due_dt = CASE 
                  WHEN LENGTH(NULLIF(TRIM(@sls_due_dt), '')) = 8 
                  THEN STR_TO_DATE(TRIM(@sls_due_dt), '%Y%m%d') 
                  ELSE NULL 
               END,
  sls_sales = NULLIF(TRIM(@sls_sales), ''),
  sls_quantity = NULLIF(TRIM(@sls_quantity), ''),
  sls_price = NULLIF(TRIM(@sls_price), '');



-- Load bronze_erp_loc_a101
LOAD DATA INFILE 'LOC_A101.csv'
INTO TABLE bronze_erp_loc_a101
FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;


ALTER TABLE bronze_erp_cust_az12 MODIFY bdate DATE;


-- Load bronze_erp_cust_az12
LOAD DATA INFILE 'CUST_AZ12_cleaned.csv'
INTO TABLE bronze_erp_cust_az12
FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(@cid, @bdate, @gen)
SET
  cid = @cid,
  bdate = NULLIF(@bdate, ''),  -- Ensure NULL for empty values
  gen = @gen;


-- Load bronze_erp_px_cat_g1v2
LOAD DATA INFILE 'PX_CAT_G1V2.csv'
INTO TABLE bronze_erp_px_cat_g1v2
FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;







