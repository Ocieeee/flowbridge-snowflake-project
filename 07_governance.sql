use role accountadmin;
use database flowbridge_dev_db;
use warehouse flowbridge_pipline_wh;


create notification integration if not exists email_notification_int
    type = email
    enabled = true
    comment = 'Email notifications Integration for pipline';

select system$start_user_email_verification('OMC03'); 

call system$send_email(
    'email_notification_int',
    'omc010101@gmail.com',
    'flowbridge Project - Data Dealer',
    'Data Dealer - Alerts are ready'
);

show integrations;

create or replace alert bronze_schema.pipline_health_alert
    warehouse = flowbridge_pipline_wh
    schedule  = '5 minute'
    comment   = 'Alerts on failed bronze loads, failed tasks and failed gold refreshes in the last hour'
if (exists (

    -- Bronze: failed file loads into RAW_ORDERS
    select 1
    from table(flowbridge_dev_db.information_schema.copy_history(
        table_name => 'FLOWBRIDGE_DEV_DB.BRONZE_SCHEMA.RAW_ORDERS',
        start_time => dateadd(hour, -1, current_timestamp())
    ))
    where upper(status) = 'LOAD FAILED'

    union all

    -- Silver: failed tasks
    select 1
    from table(flowbridge_dev_db.information_schema.task_history(
        scheduled_time_range_start => dateadd(hour, -1, current_timestamp()),
        result_limit => 1000
    ))
    where state = 'FAILED'
      and database_name = 'FLOWBRIDGE_DEV_DB'

    union all

    -- Gold: failed dynamic table refreshes
    select 1
    from table(flowbridge_dev_db.information_schema.dynamic_table_refresh_history())
    where database_name = 'FLOWBRIDGE_DEV_DB'
      and schema_name in ('GOLD_SCH', 'GOLD_SCHEMA')
      and state = 'FAILED'
      and refresh_start_time > dateadd(hour, -1, current_timestamp())

))
then
    call system$send_email(
        'email_notification_int',
        'omc010101@gmail.com',
        'Project Pipeline alert! - Data Dealer',
        'Something went wrong in the FlowBridge pipeline (bronze load, silver task or gold refresh failed in the last hour).'
    );

-- Alerts are created suspended

alter alert bronze_schema.pipline_health_alert resume;

show alerts in database flowbridge_dev_db;

show resource monitors;




