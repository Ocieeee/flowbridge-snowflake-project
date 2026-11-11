





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

show resource monitors like 'flowbridge_%'