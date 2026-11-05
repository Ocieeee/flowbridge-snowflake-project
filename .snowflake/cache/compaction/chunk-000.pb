
W
	README.md# flowbridge-snowflake-project*
ý³°× 4eea1ee2c241cd6261c59a0dbfbbe042
¾
02_bronze.sql€-------------------Environment-----------------------------------------

use database flowbridge_dev_db

use role accountadmin



----------------------------- Storage Integration -----------------------------

create storage integration if not exists flowbridge_adls_integration
    type = external_stage
    storage_provider = 'azure'
    enabled = true
    azure_tenant_id = '873303fc-145a-43b1-8241-7f31dea055f5'
    storage_allowed_locations = (
        'azure://stflowbridge.blob.core.windows.net/supply-chain-raw-dev/',
        'azure://stflowbridge.blob.core.windows.net/supply-chain-raw-prod/'
    );

describe integration flowbridge_adls_integration;



----grant to sysadmin

grant usage on integration flowbridge_adls_integration to role sysadmin;



----------------------------------------Notification integration [Event grid] -----------------------

CREATE or REPLACE notification integration flowbridge_azure_notification_int
    enabled = true
    type = queue
    notification_provider = azure_storage_queue
    azure_storage_queue_primary_uri = 'https://stflowbridge.queue.core.windows.net/flowbridge-supply-chain-queue'
    azure_tenant_id = '873303fc-145a-43b1-8241-7f31dea055f5';


describe integration flowbridge_azure_notification_int;

grant usage on integration flowbridge_azure_notification_int to role sysadmin;


-----------------------switch role sysadmin ------------------------------------------

use role sysadmin;

use database flowbridge_dev_db;
use schema flowbridge_dev_db.bronze_schema;
use warehouse FLOWBRIDGE_PIPLINE_WH;



--------------------------File Format------------------------------------------

CREATE FILE FORMAT IF NOT EXISTS bronze_schema.json_file_format
    TYPE = 'JSON'
    strip_outer_array = true  -- [ ]
    comment = 'Json file format'


describe file format bronze_schema.json_file_format


----------------------External Stage ------------------------------------------

CREATE STAGE IF NOT EXISTS bronze_schema.adls_raw_stage
    url = 'azure://stflowbridge.blob.core.windows.net/supply-chain-raw-dev/'
    storage_integration = flowbridge_adls_integration
    file_format = bronze_schema.json_file_format
    comment = 'This stage contains the raw data';


list @bronze_schema.adls_raw_stage;


-- ==================================================
-- Raw Order Table
-- Transient -- no fall safe cost efficient reloadable
-- Metadata column added at ingestion time by snowflake
-- ==================================================

create or replace transient table bronze_schema.raw_orders (
    raw_data variant,
    ingested_at timestamp_ntz default current_timestamp(),
    file_name string,
    file_row_number number,
    load_id string default uuid_string()
) comment = 'Raw Orders Table Bronze Tab supply chain';


-- =====================================
-- snowpipe
--=====================================


create pipe if not exists bronze_schema.supply_chain_pipe
    auto_ingest = true
    integration = flowbridge_azure_notification_int
    comment = 'Pipe for Supply Chain Raw Data'
as
    copy into bronze_schema.raw_orders (
        raw_data,
        file_name,
        file_row_number
    )
from (
    select $1,
             metadata$filename,
             metadata$file_row_number 
    from @bronze_schema.adls_raw_stage
) file_format = ( format_name = 'bronze_schema.json_file_format' );


show pipes;

select system$pipe_status(' bronze_schema.supply_chain_pipe');*
éÑ°× 362b24ffa8e143d8501fa58922023d7e
Ä
01_account_setup.sqlÿ





----------------------DataBase----------------------------------------------

CREATE DATABASE IF NOT EXISTS flowbridge_dev_db
    comment = 'flowbridge supply chain - Development database'


CREATE DATABASE IF NOT EXISTS flowbridge_prod_db
    comment = 'flowbridge supply chain - Production database'


------------------------Schema---------------------------------
CREATE SCHEMA IF NOT EXISTS flowbridge_dev_db.bronze_schema
    comment = 'Raw Ingestion Layer';

CREATE SCHEMA IF NOT EXISTS flowbridge_dev_db.silver_schema
    comment = 'Transformation Layer';

CREATE SCHEMA IF NOT EXISTS flowbridge_dev_db.gold_schema
    comment = 'Aggeration Layer';

CREATE SCHEMA IF NOT EXISTS flowbridge_dev_db.serving_schema
    comment = 'Serving Layer';


-------------------------------------------Prod Schema -------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS flowbridge_prod_db.bronze_schema
    comment = 'Raw Ingestion Layer';

CREATE SCHEMA IF NOT EXISTS flowbridge_prod_db.silver_schema
    comment = 'Transformation Layer';

CREATE SCHEMA IF NOT EXISTS flowbridge_prod_db.gold_schema
    comment = 'Aggeration Layer';

CREATE SCHEMA IF NOT EXISTS flowbridge_prod_db.serving_schema
    comment = 'Serving Layer';



-----------------------Warehouse---------------------------------------------------------

create warehouse if not exists flowbridge_pipline_wh
    warehouse_size = 'x-small'
    auto_suspend =  60
    auto_resume = true
    comment = 'Pipline workloads : ingestion + transformation'


create warehouse if not exists flowbridge_analytics_wh
    warehouse_size = 'x-small'
    auto_suspend =  60
    auto_resume = true
    comment = 'Pipline workloads : steamlit + data sharing'


create or replace resource monitor flowbridge_pipline_rm
    with credit_quota = 70 
    frequency = monthly
    start_timestamp = immediately
    triggers
        on 75 percent  do notify
        on 90 percent  do notify
        on 100 percent do suspend;


create or replace resource monitor flowbridge_analytics_rm
    with credit_quota = 70 
    frequency = monthly
    start_timestamp = immediately
    triggers
        on 75 percent  do notify
        on 90 percent  do notify
        on 100 percent do suspend;


    alter warehouse flowbridge_pipline_wh set resource_monitor = flowbridge_pipline_rm;

    alter warehouse flowbridge_analytics_wh set resource_monitor = flowbridge_analytics_rm;


use role accountadmin

grant execute task on account to role sysadmin

grant usage on warehouse flowbridge_pipline_wh to role sysadmin
grant usage on warehouse flowbridge_analytics_wh to role sysadmin;


grant all privileges on database flowbridge_dev_db to role sysadmin
grant all privileges on database flowbridge_prod_db to role sysadmin


grant all privileges on all schemas in database flowbridge_dev_db to role sysadmin;
grant all privileges on all schemas in database flowbridge_prod_db to role sysadmin;


show databases like 'flowbridge_%'

show warehouses like 'flowbridge_%'

show resource monitors like 'flowbridge_%'*
ý³°× eaca776d87305df6894da46ca4c754f0