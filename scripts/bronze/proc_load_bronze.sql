USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE 
        @StartTime       DATETIME2,
        @EndTime         DATETIME2,
        @DurationMs      BIGINT,
        @TableStartTime  DATETIME2,
        @TableEndTime    DATETIME2;

    BEGIN TRY
        /* ===============================
           START PROCEDURE
           =============================== */
        SET @StartTime = SYSDATETIME();

        PRINT '================================================';
        PRINT ' STARTING BRONZE LAYER LOAD ';
        PRINT ' Start Time: ' + CONVERT(VARCHAR(30), @StartTime, 121);
        PRINT '================================================';

        /* ===============================
           CRM TABLES
           =============================== */
        PRINT '------------------------------------------------';
        PRINT ' Loading CRM Tables ';
        PRINT '------------------------------------------------';

        /* ===== CRM - customer info ===== */
        PRINT '>> Loading bronze.crm_cust_info';
        SET @TableStartTime = SYSDATETIME();

        TRUNCATE TABLE bronze.crm_cust_info;

        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\rowan.reda\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @TableEndTime = SYSDATETIME();
        PRINT '   Duration (ms): ' 
              + CAST(DATEDIFF(MILLISECOND, @TableStartTime, @TableEndTime) AS VARCHAR);

        PRINT '------------------------------------------------';

        /* ===== CRM - product info ===== */
        PRINT '>> Loading bronze.crm_prd_info';
        SET @TableStartTime = SYSDATETIME();

        TRUNCATE TABLE bronze.crm_prd_info;

        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\rowan.reda\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @TableEndTime = SYSDATETIME();
        PRINT '   Duration (ms): ' 
              + CAST(DATEDIFF(MILLISECOND, @TableStartTime, @TableEndTime) AS VARCHAR);

        PRINT '------------------------------------------------';

        /* ===== CRM - sales details ===== */
        PRINT '>> Loading bronze.crm_sales_details';
        SET @TableStartTime = SYSDATETIME();

        TRUNCATE TABLE bronze.crm_sales_details;

        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\rowan.reda\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @TableEndTime = SYSDATETIME();
        PRINT '   Duration (ms): ' 
              + CAST(DATEDIFF(MILLISECOND, @TableStartTime, @TableEndTime) AS VARCHAR);

        /* ===============================
           ERP TABLES
           =============================== */
        PRINT '------------------------------------------------';
        PRINT ' Loading ERP Tables ';
        PRINT '------------------------------------------------';

        /* ===== ERP - customer az12 ===== */
        PRINT '>> Loading bronze.erp_cust_az12';
        SET @TableStartTime = SYSDATETIME();

        TRUNCATE TABLE bronze.erp_cust_az12;

        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\rowan.reda\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @TableEndTime = SYSDATETIME();
        PRINT '   Duration (ms): ' 
              + CAST(DATEDIFF(MILLISECOND, @TableStartTime, @TableEndTime) AS VARCHAR);

        PRINT '------------------------------------------------';

        /* ===== ERP - location a101 ===== */
        PRINT '>> Loading bronze.erp_loc_a101';
        SET @TableStartTime = SYSDATETIME();

        TRUNCATE TABLE bronze.erp_loc_a101;

        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\rowan.reda\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @TableEndTime = SYSDATETIME();
        PRINT '   Duration (ms): ' 
              + CAST(DATEDIFF(MILLISECOND, @TableStartTime, @TableEndTime) AS VARCHAR);

        PRINT '------------------------------------------------';

        /* ===== ERP - price category ===== */
        PRINT '>> Loading bronze.erp_px_cat_g1v2';
        SET @TableStartTime = SYSDATETIME();

        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\rowan.reda\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @TableEndTime = SYSDATETIME();
        PRINT '   Duration (ms): ' 
              + CAST(DATEDIFF(MILLISECOND, @TableStartTime, @TableEndTime) AS VARCHAR);

        /* ===============================
           END PROCEDURE
           =============================== */
        SET @EndTime = SYSDATETIME();
        SET @DurationMs = DATEDIFF(MILLISECOND, @StartTime, @EndTime);

        PRINT '================================================';
        PRINT ' BRONZE LOAD COMPLETED SUCCESSFULLY ';
        PRINT ' Total Duration (ms): ' + CAST(@DurationMs AS VARCHAR);
        PRINT ' End Time: ' + CONVERT(VARCHAR(30), @EndTime, 121);
        PRINT '================================================';
    END TRY
    BEGIN CATCH
        PRINT '================================================';
        PRINT ' ERROR DURING BRONZE LOAD ';
        PRINT ' Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR);
        PRINT ' Error Severity: ' + CAST(ERROR_SEVERITY() AS VARCHAR);
        PRINT ' Error State: ' + CAST(ERROR_STATE() AS VARCHAR);
        PRINT ' Error Line: ' + CAST(ERROR_LINE() AS VARCHAR);
        PRINT ' Error Message: ' + ERROR_MESSAGE();
        PRINT '================================================';

        THROW; -- rethrow error for job/agent visibility
    END CATCH
END;
GO

