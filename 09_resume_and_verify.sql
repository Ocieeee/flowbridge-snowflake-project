use role sysadmin;
use warehouse flowbridge_pipline_wh;


--- 1 --------resume pipe ----
alter pipe flowbridge_prod_db.bronze_schema.supply_chain_pipe_prod set 
pipe_execution_paused = false;


select system$pipe_status('flowbridge_prod_db.bronze_schema.supply_chain_pipe_prod');

--- 2 tasks----
alter task FLOWBRIDGE_PROD_DB.SILVER_SCHEMA.BRONZE_TO_SILVER_TASK resume;
alter task FLOWBRIDGE_PROD_DB.SILVER_SCHEMA.SILVER_TO_STAR_TASK resume;

show tasks;

---- 3 resume alerts
use role accountadmin;

alter alert flowbridge.prod_db.bronze_schema.pipeline_health_alert resume;
show tasks in database


---- 4 create stream -----------
use role sysadmin
create or replace stream flowbridge_prod_db.silver_schema.raw_orders_stream
    on table flowbridge_prod_db.bronze_schema.raw_orders
    append_only = true
    show_initial_rows = true
    comment = 'Stream on raw for silver proccing' ;


----- 4 resume dynaminx table
alter dynamic table FLOWBRIDGE_PROD_DB.GOLD_SCHEMA.AGG_BASE resume;
alter dynamic table FLOWBRIDGE_PROD_DB.GOLD_SCHEMA.AGG_INVENTORY_TURNOVER resume;
alter dynamic table FLOWBRIDGE_PROD_DB.GOLD_SCHEMA.AGG_ORDER_FULFILLMENT resume;
alter dynamic table FLOWBRIDGE_PROD_DB.GOLD_SCHEMA.AGG_SHIPMENT_DELAYS resume;
alter dynamic table FLOWBRIDGE_PROD_DB.GOLD_SCHEMA.AGG_SUPPLIER_PERFORMANCE resume;


--- 5 counts data--------

select 'BRONZE' as layer, 'RAW_ORDERS' as table_name, count(*) as row_count from flowbridge_prod_db.bronze_schema.raw_orders
union all
select 'SILVER', 'STG_ORDERS',    count(*) from flowbridge_prod_db.silver_schema.stg_orders
union all
select 'SILVER', 'DEAD_LETTER',   count(*) from flowbridge_prod_db.silver_schema.dead_letter
union all
select 'SILVER', 'DIM_CUSTOMER',  count(*) from flowbridge_prod_db.silver_schema.dim_customer
union all
select 'SILVER', 'DIM_PRODUCT',   count(*) from flowbridge_prod_db.silver_schema.dim_product
union all
select 'SILVER', 'DIM_SUPPLIER',  count(*) from flowbridge_prod_db.silver_schema.dim_supplier
union all
select 'SILVER', 'DIM_WAREHOUSE', count(*) from flowbridge_prod_db.silver_schema.dim_warehouse
union all
select 'SILVER', 'DIM_SHIPMENT',  count(*) from flowbridge_prod_db.silver_schema.dim_shipment
union all
select 'SILVER', 'FACT_ORDERS',   count(*) from flowbridge_prod_db.silver_schema.fact_orders
union all
select 'GOLD',   'AGG_BASE',                count(*) from flowbridge_prod_db.gold_schema.agg_base
union all
select 'GOLD',   'AGG_ORDER_FULFILLMENT',   count(*) from flowbridge_prod_db.gold_schema.agg_order_fulfillment
union all
select 'GOLD',   'AGG_SUPPLIER_PERFORMANCE',count(*) from flowbridge_prod_db.gold_schema.agg_supplier_performance
union all
select 'GOLD',   'AGG_INVENTORY_TURNOVER',  count(*) from flowbridge_prod_db.gold_schema.agg_inventory_turnover
union all
select 'GOLD',   'AGG_SHIPMENT_DELAYS',     count(*) from flowbridge_prod_db.gold_schema.agg_shipment_delays





---- 6 Verifiy view ----
select * from FLOWBRIDGE_PROD_DB.SERVING_SCHEMA.VW_INVENTORY_TURNOVER

select * from FLOWBRIDGE_PROD_DB.SERVING_SCHEMA.VW_ORDER_FULFILLMENT

select * from FLOWBRIDGE_PROD_DB.SERVING_SCHEMA.VW_SHIPMENT_DELAYS

select * from FLOWBRIDGE_PROD_DB.SERVING_SCHEMA.VW_SUPPLIER_PERFORMANCE