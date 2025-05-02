ELIMITER $$

CREATE PROCEDURE load_silver()
BEGIN
    DECLARE start_time DATETIME;
    DECLARE end_time DATETIME;

    -- crm_cust_info
    SET start_time = NOW();
    TRUNCATE TABLE silver.crm_cust_info;
    INSERT INTO silver.crm_cust_info (
        cst_id, cst_key, cst_firstname, cst_lastname,
        cst_marital_status, cst_gndr, cst_create_date
    )
    SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname),
        TRIM(cst_lastname),
        CASE 
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'n/a'
        END,
        CASE 
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END,
        cst_create_date
    FROM bronze.crm_cust_info b
    WHERE cst_id IS NOT NULL
      AND cst_create_date = (
          SELECT MAX(cst_create_date)
          FROM bronze.crm_cust_info
          WHERE cst_id = b.cst_id
      );
    SET end_time = NOW();
    SELECT 'crm_cust_info duration', TIMESTAMPDIFF(SECOND, start_time, end_time);

    -- crm_prd_info
    SET start_time = NOW();
    TRUNCATE TABLE silver.crm_prd_info;
    INSERT INTO silver.crm_prd_info (
        prd_id, cat_id, prd_key, prd_nm,
        prd_cost, prd_line, prd_start_dt, prd_end_dt
    )
    SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_'),
        SUBSTRING(prd_key, 7),
        prd_nm,
        IFNULL(prd_cost, 0),
        CASE 
            WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            ELSE 'n/a'
        END,
        CAST(prd_start_dt AS DATE),
        NULL -- prd_end_dt logic to be implemented if necessary
    FROM bronze.crm_prd_info;
    SET end_time = NOW();
    SELECT 'crm_prd_info duration', TIMESTAMPDIFF(SECOND, start_time, end_time);

    -- crm_sales_details
    SET start_time = NOW();
    TRUNCATE TABLE silver.crm_sales_details;
    INSERT INTO silver.crm_sales_details (
        sls_ord_num, sls_prd_key, sls_cust_id,
        sls_order_dt, sls_ship_dt, sls_due_dt,
        sls_sales, sls_quantity, sls_price
    )
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE 
            WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt) != 8 THEN NULL
            ELSE STR_TO_DATE(sls_order_dt, '%Y%m%d')
        END,
        CASE 
            WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt) != 8 THEN NULL
            ELSE STR_TO_DATE(sls_ship_dt, '%Y%m%d')
        END,
        CASE 
            WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt) != 8 THEN NULL
            ELSE STR_TO_DATE(sls_due_dt, '%Y%m%d')
        END,
        CASE 
            WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END,
        sls_quantity,
        CASE 
            WHEN sls_price IS NULL OR sls_price <= 0
                THEN sls_sales / NULLIF(sls_quantity, 0)
            ELSE sls_price
        END
    FROM bronze.crm_sales_details;
    SET end_time = NOW();
    SELECT 'crm_sales_details duration', TIMESTAMPDIFF(SECOND, start_time, end_time);

    -- erp_cust_az12
    SET start_time = NOW();
    TRUNCATE TABLE silver.erp_cust_az12;
    INSERT INTO silver.erp_cust_az12 (
        cid, bdate, gen
    )
    SELECT
        IF(cid LIKE 'NAS%', SUBSTRING(cid, 4), cid),
        IF(bdate > NOW(), NULL, bdate),
        CASE 
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'n/a'
        END
    FROM bronze.erp_cust_az12;
    SET end_time = NOW();
    SELECT 'erp_cust_az12 duration', TIMESTAMPDIFF(SECOND, start_time, end_time);

    -- erp_loc_a101
    SET start_time = NOW();
    TRUNCATE TABLE silver.erp_loc_a101;
    INSERT INTO silver.erp_loc_a101 (
        cid, cntry
    )
    SELECT
        REPLACE(cid, '-', ''),
        CASE 
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
            ELSE TRIM(cntry)
        END
    FROM bronze.erp_loc_a101;
    SET end_time = NOW();
    SELECT 'erp_loc_a101 duration', TIMESTAMPDIFF(SECOND, start_time, end_time);

    -- erp_px_cat_g1v2
    SET start_time = NOW();
    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    INSERT INTO silver.erp_px_cat_g1v2 (
        id, cat, subcat, maintenance
    )
    SELECT id, cat, subcat, maintenance
    FROM bronze.erp_px_cat_g1v2;
    SET end_time = NOW();
    SELECT 'erp_px_cat_g1v2 duration', TIMESTAMPDIFF(SECOND, start_time, end_time);

END$$

DELIMITER ;
