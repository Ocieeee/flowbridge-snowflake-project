

set env = 'DEV';;
set DB =  'FLOWBRIDGE_'||$env||'_DB';

use database identifier($DB);

use role sysadmin;
use schema silver_schema;
use warehouse flowbridge_pipline_wh;


create or replace transient table  silver_schema.dead_letter (
    raw_data variant,
    error_reason string,
    file_name string,
    file_row_number number,
    rejected_at timestamp_ntz default current_timestamp()
) comment = 'Dead letter table for bad records';


CREATE OR REPLACE TABLE silver_schema.stg_orders (
    -- order
    ORDER_ID              STRING,
    ORDER_DATE            TIMESTAMP_NTZ,
    ORDER_STATUS          STRING,

    -- customer
    CUSTOMER_ID           STRING,
    CUSTOMER_NAME         STRING,          -- nullable, null in sample
    CUSTOMER_REGION       STRING,
    CUSTOMER_SEGMENT      STRING,

    -- supplier
    SUPPLIER_ID           STRING,
    SUPPLIER_NAME         STRING,
    SUPPLIER_COUNTRY      STRING,
    LEAD_TIME_DAYS        NUMBER,
    PERFORMANCE_SCORE     FLOAT,

    -- shipment
    SHIPMENT_ID           STRING,
    CARRIER               STRING,
    SHIP_DATE             TIMESTAMP_NTZ,
    ESTIMATED_DELIVERY    DATE,
    DELAY_DAYS            NUMBER,

    -- product
    PRODUCT_ID            STRING,
    PRODUCT_NAME          STRING,
    CATEGORY              STRING,
    QUANTITY              NUMBER,
    UNIT_PRICE            FLOAT,

    -- financials
    TOTAL_AMOUNT          float,
    PAYMENT_STATUS        STRING,

    -- warehouse
    WAREHOUSE_ID          STRING,
    WAREHOUSE_LOCATION    STRING,
    INVENTORY_LEVEL       NUMBER,

    --metadata
    FILE_NAME               STRING,    
    FILE_ROW_NUMBER         NUMBER,    
    INGESTED_AT             TIMESTAMP_NTZ,
    TRANSFORMED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    
) COMMENT = 'Silver stage - 1 ';


create or replace stream silver_schema.raw_orders_stream
    on table flowbridge_dev_db.bronze_schema.raw_orders
    append_only = true
    show_initial_rows = true
    comment = 'Stream on raw for silver proccing' ;

show streams in schema;

create or replace procedure silver_schema.sp_bronze_to_silver()
    returns string
    language sql
    comment = 'SP routes bad record to dead letter'
as
BEGIN
        create or replace temporary table silver_schema.stream_buffer as
        select * from silver_schema.raw_orders_stream
        where metadata$action = 'INSERT';
    
        INSERT INTO SILVER_SCHEMA.dead_letter(
            RAW_DATA,
            ERROR_REASON,
            FILE_NAME,
            FILE_ROW_NUMBER
        )
       SELECT
        S.RAW_DATA,
        CASE
            -- Order level validations
            WHEN S.RAW_DATA:order_id::STRING IS NULL
                THEN 'Missing order_id'
            WHEN TRY_TO_TIMESTAMP_NTZ(S.RAW_DATA:order_date::STRING) IS NULL
                 AND S.RAW_DATA:order_date::STRING NOT LIKE '__-__-____'
                THEN 'Invalid or missing order_date'
            WHEN UPPER(TRIM(S.RAW_DATA:order_status::STRING)) NOT IN
                 ('PENDING', 'PROCESSING', 'SHIPPED', 'IN TRANSIT', 'DELIVERED', 'CANCELLED')
                THEN 'Invalid order_status: ' || COALESCE(S.RAW_DATA:order_status::STRING, 'NULL')
    
            -- Customer level validations
            WHEN S.RAW_DATA:customer.customer_id::STRING IS NULL
                THEN 'Missing customer_id'
    
            -- Supplier level validations
            WHEN S.RAW_DATA:supplier.supplier_id::STRING IS NULL
                THEN 'Missing supplier_id'
            WHEN S.RAW_DATA:supplier.performance_score::FLOAT > 100
                THEN 'Invalid performance_score > 100: ' || S.RAW_DATA:supplier.performance_score::STRING
            WHEN S.RAW_DATA:supplier.performance_score::FLOAT < 0
                THEN 'Invalid performance_score < 0: ' || S.RAW_DATA:supplier.performance_score::STRING
            WHEN S.RAW_DATA:supplier.lead_time_days::NUMBER < 0
                THEN 'Invalid lead_time_days < 0: ' || S.RAW_DATA:supplier.lead_time_days::STRING
    
            -- Product level validations
            WHEN S.RAW_DATA:items[0].product_id::STRING IS NULL
                THEN 'Missing product_id'
            WHEN S.RAW_DATA:items[0].quantity::NUMBER IS NULL
                 OR S.RAW_DATA:items[0].quantity::NUMBER <= 0
                THEN 'Invalid quantity: ' || COALESCE(S.RAW_DATA:items[0].quantity::STRING, 'NULL')
            WHEN S.RAW_DATA:items[0].unit_price::FLOAT IS NULL
                 OR S.RAW_DATA:items[0].unit_price::FLOAT < 0
                THEN 'Invalid unit_price: ' || COALESCE(S.RAW_DATA:items[0].unit_price::STRING, 'NULL')
    
            -- Financial level validations
            WHEN S.RAW_DATA:financials.total_amount::FLOAT IS NULL
                 OR S.RAW_DATA:financials.total_amount::FLOAT < 0
                THEN 'Invalid total_amount: ' || COALESCE(S.RAW_DATA:financials.total_amount::STRING, 'NULL')
    
            -- Warehouse level validations
            WHEN S.RAW_DATA:warehouse.inventory_level::NUMBER < 0
                THEN 'Invalid inventory_level < 0: ' || S.RAW_DATA:warehouse.inventory_level::STRING
    
            ELSE 'Unknown'
        END AS ERROR_REASON,
        S.FILE_NAME,
        S.FILE_ROW_NUMBER
    FROM SILVER_SCHEMA.STREAM_BUFFER AS S
    WHERE
    (
        -- Order level
        S.RAW_DATA:order_id::STRING IS NULL
        OR (
            TRY_TO_TIMESTAMP_NTZ(S.RAW_DATA:order_date::STRING) IS NULL
            AND S.RAW_DATA:order_date::STRING NOT LIKE '__-__-____'
        )
        OR UPPER(TRIM(S.RAW_DATA:order_status::STRING)) NOT IN (
            'PENDING', 'PROCESSING', 'SHIPPED',
            'IN TRANSIT', 'DELIVERED', 'CANCELLED'
        )
        -- Customer level
        OR S.RAW_DATA:customer.customer_id::STRING IS NULL
        -- Supplier level
        OR S.RAW_DATA:supplier.supplier_id::STRING IS NULL
        OR S.RAW_DATA:supplier.performance_score::FLOAT > 100
        OR S.RAW_DATA:supplier.performance_score::FLOAT < 0
        OR S.RAW_DATA:supplier.lead_time_days::NUMBER < 0
        -- Product level
        OR S.RAW_DATA:items[0].product_id::STRING IS NULL
        OR S.RAW_DATA:items[0].quantity::NUMBER IS NULL
        OR S.RAW_DATA:items[0].quantity::NUMBER <= 0
        OR S.RAW_DATA:items[0].unit_price::FLOAT IS NULL
        OR S.RAW_DATA:items[0].unit_price::FLOAT < 0
        -- Financial level
        OR S.RAW_DATA:financials.total_amount::FLOAT IS NULL
        OR S.RAW_DATA:financials.total_amount::FLOAT < 0
        -- Warehouse level
        OR S.RAW_DATA:warehouse.inventory_level::NUMBER < 0
    );
    
    
    MERGE INTO SILVER_SCHEMA.STG_ORDERS AS TGT
    USING (
        SELECT
            -- Order fields
            UPPER(TRIM(S.RAW_DATA:order_id::STRING)) AS ORDER_ID,
            TRY_TO_TIMESTAMP_NTZ(
                CASE
                    WHEN S.RAW_DATA:order_date::STRING LIKE '__-__-____'
                        THEN TO_VARCHAR(
                                 TO_DATE(S.RAW_DATA:order_date::STRING, 'DD-MM-YYYY'),
                                 'YYYY-MM-DD'
                             ) || 'T00:00:00Z'
                    ELSE S.RAW_DATA:order_date::STRING
                END
            ) AS ORDER_DATE,
            UPPER(TRIM(S.RAW_DATA:order_status::STRING)) AS ORDER_STATUS,
    
            -- Customer fields
            UPPER(TRIM(S.RAW_DATA:customer.customer_id::STRING)) AS CUSTOMER_ID,
            COALESCE(UPPER(TRIM(S.RAW_DATA:customer.customer_name::STRING)), 'UNKNOWN') AS CUSTOMER_NAME,
            COALESCE(UPPER(TRIM(S.RAW_DATA:customer.region::STRING)), 'UNKNOWN')        AS CUSTOMER_REGION,
            COALESCE(UPPER(TRIM(S.RAW_DATA:customer.segment::STRING)), 'UNKNOWN')       AS CUSTOMER_SEGMENT,
    
            -- Supplier fields
            UPPER(TRIM(S.RAW_DATA:supplier.supplier_id::STRING)) AS SUPPLIER_ID,
            COALESCE(UPPER(TRIM(S.RAW_DATA:supplier.supplier_name::STRING)), 'UNKNOWN') AS SUPPLIER_NAME,
            COALESCE(UPPER(TRIM(S.RAW_DATA:supplier.country::STRING)), 'UNKNOWN')       AS SUPPLIER_COUNTRY,
            COALESCE(S.RAW_DATA:supplier.lead_time_days::NUMBER, 0)                     AS LEAD_TIME_DAYS,
            COALESCE(S.RAW_DATA:supplier.performance_score::FLOAT, 0)                   AS PERFORMANCE_SCORE,
    
            -- Shipment fields
            COALESCE(UPPER(TRIM(S.RAW_DATA:shipment.shipment_id::STRING)), 'UNKNOWN') AS SHIPMENT_ID,
            COALESCE(UPPER(TRIM(S.RAW_DATA:shipment.carrier::STRING)), 'UNKNOWN')     AS CARRIER,
            TRY_TO_TIMESTAMP_NTZ(S.RAW_DATA:shipment.ship_date::STRING)               AS SHIP_DATE,
            TRY_TO_DATE(S.RAW_DATA:shipment.estimated_delivery::STRING)               AS ESTIMATED_DELIVERY,
            GREATEST(COALESCE(S.RAW_DATA:shipment.delay_days::NUMBER, 0), 0)          AS DELAY_DAYS,
    
            -- Product fields (items[0] = first array element)
            UPPER(TRIM(S.RAW_DATA:items[0].product_id::STRING))                            AS PRODUCT_ID,
            COALESCE(UPPER(TRIM(S.RAW_DATA:items[0].product_name::STRING)), 'UNKNOWN')     AS PRODUCT_NAME,
            COALESCE(UPPER(TRIM(S.RAW_DATA:items[0].category::STRING)), 'UNKNOWN')         AS CATEGORY,
            S.RAW_DATA:items[0].quantity::NUMBER                                           AS QUANTITY,
            S.RAW_DATA:items[0].unit_price::FLOAT                                          AS UNIT_PRICE,
    
            -- Financial fields
            S.RAW_DATA:financials.total_amount::FLOAT                                      AS TOTAL_AMOUNT,
            COALESCE(UPPER(TRIM(S.RAW_DATA:financials.payment_status::STRING)), 'UNKNOWN') AS PAYMENT_STATUS,
    
            -- Warehouse fields
            COALESCE(UPPER(TRIM(S.RAW_DATA:warehouse.warehouse_id::STRING)), 'UNKNOWN')       AS WAREHOUSE_ID,
            COALESCE(UPPER(TRIM(S.RAW_DATA:warehouse.warehouse_location::STRING)), 'UNKNOWN') AS WAREHOUSE_LOCATION,
            COALESCE(S.RAW_DATA:warehouse.inventory_level::NUMBER, 0)                         AS INVENTORY_LEVEL,
    
            -- Metadata
            S.FILE_NAME,
            S.FILE_ROW_NUMBER,
            S.INGESTED_AT
        FROM SILVER_SCHEMA.STREAM_BUFFER AS S
        WHERE
            S.RAW_DATA:order_id::STRING IS NOT NULL
            AND (
                TRY_TO_TIMESTAMP_NTZ(S.RAW_DATA:order_date::STRING) IS NOT NULL
                OR S.RAW_DATA:order_date::STRING LIKE '__-__-____'
            )
            AND UPPER(TRIM(S.RAW_DATA:order_status::STRING)) IN (
                'PENDING', 'PROCESSING', 'SHIPPED',
                'IN TRANSIT', 'DELIVERED', 'CANCELLED'
            )
            AND S.RAW_DATA:customer.customer_id::STRING IS NOT NULL
            AND S.RAW_DATA:supplier.supplier_id::STRING IS NOT NULL
            AND S.RAW_DATA:supplier.performance_score::FLOAT BETWEEN 0 AND 100
            AND (
                S.RAW_DATA:supplier.lead_time_days::NUMBER IS NULL
                OR S.RAW_DATA:supplier.lead_time_days::NUMBER >= 0
            )
            AND S.RAW_DATA:items[0].product_id::STRING IS NOT NULL
            AND S.RAW_DATA:items[0].quantity::NUMBER IS NOT NULL
            AND S.RAW_DATA:items[0].quantity::NUMBER > 0
            AND S.RAW_DATA:items[0].unit_price::FLOAT IS NOT NULL
            AND S.RAW_DATA:items[0].unit_price::FLOAT >= 0
            AND S.RAW_DATA:financials.total_amount::FLOAT IS NOT NULL
            AND S.RAW_DATA:financials.total_amount::FLOAT >= 0
            AND (
                S.RAW_DATA:warehouse.inventory_level::NUMBER IS NULL
                OR S.RAW_DATA:warehouse.inventory_level::NUMBER >= 0
            )
    ) AS SRC
    ON TGT.ORDER_ID = SRC.ORDER_ID   -- assumed: this part wasn't in your screenshots
    WHEN MATCHED THEN UPDATE SET
        TGT.ORDER_STATUS       = SRC.ORDER_STATUS,
        TGT.CARRIER            = SRC.CARRIER,
        TGT.SHIP_DATE          = SRC.SHIP_DATE,
        TGT.ESTIMATED_DELIVERY = SRC.ESTIMATED_DELIVERY,
        TGT.DELAY_DAYS         = SRC.DELAY_DAYS,
        TGT.INVENTORY_LEVEL    = SRC.INVENTORY_LEVEL,
        TGT.PAYMENT_STATUS     = SRC.PAYMENT_STATUS,
        TGT.CUSTOMER_NAME      = SRC.CUSTOMER_NAME,
        TGT.CUSTOMER_REGION    = SRC.CUSTOMER_REGION,
        TGT.SUPPLIER_NAME      = SRC.SUPPLIER_NAME,
        TGT.PERFORMANCE_SCORE  = SRC.PERFORMANCE_SCORE,
        TGT.WAREHOUSE_ID       = SRC.WAREHOUSE_ID,
        TGT.WAREHOUSE_LOCATION = SRC.WAREHOUSE_LOCATION,
        TGT.TRANSFORMED_AT     = CURRENT_TIMESTAMP()
    
    WHEN NOT MATCHED THEN INSERT (
        ORDER_ID, ORDER_DATE, ORDER_STATUS,
        CUSTOMER_ID, CUSTOMER_NAME, CUSTOMER_REGION, CUSTOMER_SEGMENT,
        SUPPLIER_ID, SUPPLIER_NAME, SUPPLIER_COUNTRY, LEAD_TIME_DAYS, PERFORMANCE_SCORE,
        SHIPMENT_ID, CARRIER, SHIP_DATE, ESTIMATED_DELIVERY, DELAY_DAYS,
        PRODUCT_ID, PRODUCT_NAME, CATEGORY, QUANTITY, UNIT_PRICE,
        TOTAL_AMOUNT, PAYMENT_STATUS,
        WAREHOUSE_ID, WAREHOUSE_LOCATION, INVENTORY_LEVEL,
        FILE_NAME, FILE_ROW_NUMBER, INGESTED_AT
    ) VALUES (
        SRC.ORDER_ID, SRC.ORDER_DATE, SRC.ORDER_STATUS,
        SRC.CUSTOMER_ID, SRC.CUSTOMER_NAME, SRC.CUSTOMER_REGION, SRC.CUSTOMER_SEGMENT,
        SRC.SUPPLIER_ID, SRC.SUPPLIER_NAME, SRC.SUPPLIER_COUNTRY, SRC.LEAD_TIME_DAYS, SRC.PERFORMANCE_SCORE,
        SRC.SHIPMENT_ID, SRC.CARRIER, SRC.SHIP_DATE, SRC.ESTIMATED_DELIVERY, SRC.DELAY_DAYS,
        SRC.PRODUCT_ID, SRC.PRODUCT_NAME, SRC.CATEGORY, SRC.QUANTITY, SRC.UNIT_PRICE,
        SRC.TOTAL_AMOUNT, SRC.PAYMENT_STATUS,
        SRC.WAREHOUSE_ID, SRC.WAREHOUSE_LOCATION, SRC.INVENTORY_LEVEL,
        SRC.FILE_NAME, SRC.FILE_ROW_NUMBER, SRC.INGESTED_AT
    );
    
    DROP TABLE IF EXISTS SILVER_SCHEMA.STREAM_BUFFER;
    
    RETURN 'sp_bronze to silver completed successfully';

END;


create or replace task silver_schema.bronze_to_silver_task
    warehouse = FLOWBRIDGE_PIPLINE_WH
    schedule = '1 minute'
    comment = 'Task routes bad records to dead letter'
when system$stream_has_data('silver_schema.raw_orders_stream')
as
    CALL FLOWBRIDGE_DEV_DB.SILVER_SCHEMA.SP_BRONZE_TO_SILVER();


alter task silver_schema.bronze_to_silver_task resume;

show tasks;


CALL FLOWBRIDGE_DEV_DB.SILVER_SCHEMA.SP_BRONZE_TO_SILVER();

SELECT SYSTEM$STREAM_HAS_DATA('SILVER_SCHEMA.RAW_ORDERS_STREAM');


select count(*) as stg_count from silver_schema.stg_orders
union all
select count(*) as stg_count from silver_schema.dead_letter;


select * from silver_schema.dead_letter;

select * from silver_schema.stg_orders