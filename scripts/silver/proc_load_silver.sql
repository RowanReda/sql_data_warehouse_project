EXEC silver.load_silver
CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE 
        @total_start DATETIME2 = SYSDATETIME(),
        @step_start DATETIME2,
        @step_end DATETIME2;

    PRINT '============================================================';
    PRINT 'Starting silver.load_silver stored procedure...';
    PRINT 'Start Time: ' + CONVERT(VARCHAR, @total_start, 120);
    PRINT '============================================================';


    BEGIN TRY
        ------------------------------------------------------------
        PRINT 'Step 1: Loading silver.crm_cust_info...';
        ------------------------------------------------------------

        SET @step_start = SYSDATETIME();

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
            CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                 ELSE 'N/A' END,
            CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                 ELSE 'N/A' END,
            cst_create_date
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY cst_id 
                       ORDER BY cst_create_date DESC
                   ) AS flag_last
            FROM bronze.crm_cust_info
        ) t
        WHERE flag_last = 1;

        SET @step_end = SYSDATETIME();
        PRINT 'crm_cust_info loaded successfully.';
        PRINT 'Duration: ' + CAST(DATEDIFF(SECOND, @step_start, @step_end) AS VARCHAR) + ' seconds';
        PRINT '------------------------------------------------------------';


        ------------------------------------------------------------
        PRINT 'Step 2: Loading silver.crm_prd_info...';
        ------------------------------------------------------------

        SET @step_start = SYSDATETIME();

        TRUNCATE TABLE silver.crm_prd_info;

        INSERT INTO silver.crm_prd_info (
            prd_id, cat_id, prd_key, prd_nm, prd_cost,
            prd_line, prd_start_dt, prd_end_dt
        )
        SELECT
            prd_id,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_'),
            SUBSTRING(prd_key, 7, LEN(prd_key)),
            prd_nm,
            ISNULL(prd_cost, 0),
            CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
                 WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
                 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
                 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
                 ELSE 'n/a' END,
            CAST(prd_start_dt AS DATE),
            CAST(LEAD(prd_start_dt) OVER (
                    PARTITION BY prd_key 
                    ORDER BY prd_start_dt
            ) - 1 AS DATE)
        FROM bronze.crm_prd_info;

        SET @step_end = SYSDATETIME();
        PRINT 'crm_prd_info loaded successfully.';
        PRINT 'Duration: ' + CAST(DATEDIFF(SECOND, @step_start, @step_end) AS VARCHAR) + ' seconds';
        PRINT '------------------------------------------------------------';


        ------------------------------------------------------------
        PRINT 'Step 3: Loading silver.crm_sales_details...';
        ------------------------------------------------------------

        SET @step_start = SYSDATETIME();

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
            CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
                 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) END,
            CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
                 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) END,
            CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
                 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) END,
            CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
                 THEN sls_quantity * ABS(sls_price)
                 ELSE sls_sales END,
            sls_quantity,
            CASE WHEN sls_price IS NULL OR sls_price <= 0
                 THEN sls_sales / NULLIF(sls_quantity, 0)
                 ELSE sls_price END
        FROM bronze.crm_sales_details;

        SET @step_end = SYSDATETIME();
        PRINT 'crm_sales_details loaded successfully.';
        PRINT 'Duration: ' + CAST(DATEDIFF(SECOND, @step_start, @step_end) AS VARCHAR) + ' seconds';
        PRINT '------------------------------------------------------------';


        ------------------------------------------------------------
        PRINT 'Step 4: Loading silver.erp_cust_az12...';
        ------------------------------------------------------------

        SET @step_start = SYSDATETIME();

        TRUNCATE TABLE silver.erp_cust_az12;

        INSERT INTO silver.erp_cust_az12 (
            cid, bdate, gen
        )
        SELECT
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
                 ELSE cid END,
            CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END,
            CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                 ELSE 'n/a' END
        FROM bronze.erp_cust_az12;

        SET @step_end = SYSDATETIME();
        PRINT 'erp_cust_az12 loaded successfully.';
        PRINT 'Duration: ' + CAST(DATEDIFF(SECOND, @step_start, @step_end) AS VARCHAR) + ' seconds';
        PRINT '------------------------------------------------------------';


        ------------------------------------------------------------
        PRINT 'Step 5: Loading silver.erp_loc_a101...';
        ------------------------------------------------------------

        SET @step_start = SYSDATETIME();

        TRUNCATE TABLE silver.erp_loc_a101;

        INSERT INTO silver.erp_loc_a101 (cid, cntry)
        SELECT
            REPLACE(cid, '-', ''),
            CASE
                WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
                WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
                ELSE TRIM(cntry)
            END
        FROM bronze.erp_loc_a101;

        SET @step_end = SYSDATETIME();
        PRINT 'erp_loc_a101 loaded successfully.';
        PRINT 'Duration: ' + CAST(DATEDIFF(SECOND, @step_start, @step_end) AS VARCHAR) + ' seconds';
        PRINT '------------------------------------------------------------';


        ------------------------------------------------------------
        -- TOTAL DURATION
        ------------------------------------------------------------
        DECLARE @total_end DATETIME2 = SYSDATETIME();

        PRINT '============================================================';
        PRINT 'silver.load_silver COMPLETED SUCCESSFULLY';
        PRINT 'Total Duration: ' + CAST(DATEDIFF(SECOND, @total_start, @total_end) AS VARCHAR) + ' seconds';
        PRINT 'End Time: ' + CONVERT(VARCHAR, @total_end, 120);
        PRINT '============================================================';
    END TRY


    BEGIN CATCH
        PRINT '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!';
        PRINT 'ERROR in silver.load_silver';
        PRINT 'Message: ' + ERROR_MESSAGE();
        PRINT 'Line: ' + CAST(ERROR_LINE() AS VARCHAR);
        PRINT '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!';

        THROW; 
    END CATCH
END;
GO
