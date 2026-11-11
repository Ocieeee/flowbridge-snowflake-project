-------------------Environment-----------------------------------------

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

SHOW PIPES LIKE 'supply_chain_pipe' IN ACCOUNT;

select system$pipe_status('bronze_schema.supply_chain_pipe');

alter pipe bronze_schema.supply_chain_pipe refresh;

select * from bronze_schema.raw_orders;

select * 
    from table(information_schema.copy_history(
        table_name => 'raw_orders',
        start_time => dateadd(hours,-1, current_timestamp())
    ));