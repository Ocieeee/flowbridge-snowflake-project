use role sysadmin;
use warehouse flowbridge_pipline_wh;
use database flowbridge_dev_db;
use schema flowbridge_dev_db.gold_schema;

grant usage , operate on warehouse flowbridge_pipline_wh to role sysadmin;

create or replace dynamic table gold_schema.agg_base
    lag = '1 minute'
    warehouse = flowbridge_pipline_wh
    comment = 'Gold base table - fact_orders joined to all dimensions'
as
    select
    -- order / fact
    f.order_id                  as ORDER_ID,
    f.order_date                as ORDER_DATE,
    f.order_status              as ORDER_STATUS,
    f.payment_status            as PAYMENT_STATUS,

    -- measures (from the fact, so the order-time price is kept)
    f.quantity                  as QUANTITY,
    f.unit_price                as UNIT_PRICE,
    f.total_amount              as TOTAL_AMOUNT,
    f.delay_days                as DELAY_DAYS,
    f.inventory_level           as INVENTORY_LEVEL,
    
    --pipline metadata------
    f.ingested_at               as INGESTED_AT,

    -- customer
    dc.customer_id              as CUSTOMER_ID,
    dc.customer_name            as CUSTOMER_NAME,
    dc.customer_region          as CUSTOMER_REGION,
    dc.customer_segment         as CUSTOMER_SEGMENT,

    -- product
    dp.product_id               as PRODUCT_ID,
    dp.product_name             as PRODUCT_NAME,
    dp.category                 as CATEGORY,

    -- supplier
    ds.supplier_id              as SUPPLIER_ID,
    ds.supplier_name            as SUPPLIER_NAME,
    ds.supplier_country         as SUPPLIER_COUNTRY,
    ds.lead_time_days           as LEAD_TIME_DAYS,
    ds.performance_score        as PERFORMANCE_SCORE,

    -- warehouse
    dw.warehouse_id             as WAREHOUSE_ID,
    dw.warehouse_location       as WAREHOUSE_LOCATION,

    -- shipment (may be missing, shipment_sk = -1)
    dsh.shipment_id             as SHIPMENT_ID,
    dsh.carrier                 as CARRIER,
    dsh.ship_date               as SHIP_DATE,
    dsh.estimated_delivery      as ESTIMATED_DELIVERY,
from silver_schema.fact_orders f
join silver_schema.dim_customer  dc  on dc.customer_sk  = f.customer_sk
join silver_schema.dim_product   dp  on dp.product_sk   = f.product_sk
join silver_schema.dim_supplier  ds  on ds.supplier_sk  = f.supplier_sk
join silver_schema.dim_warehouse dw  on dw.warehouse_sk = f.warehouse_sk
left  join silver_schema.dim_shipment  dsh on dsh.shipment_sk = f.shipment_sk;


select * from gold_schema.agg_base limit 10;


create or replace dynamic table gold_schema.agg_order_fulfillment
    target_lag = downstream
    warehouse  = flowbridge_pipline_wh
    comment    = 'Order fulfillment KPIs by customer region. DOWNSTREAM from AGG_BASE.'
as
select
    customer_region,
    customer_segment,
    
    -- Order counts
    count(order_id)                                                       as total_orders,
    count(case when order_status = 'DELIVERED' then 1 end)                as delivered_orders,
    count(case when order_status in ('PENDING','PROCESSING') then 1 end)  as pending_orders,
    count(case when order_status = 'CANCELLED' then 1 end)                as cancelled_orders,
    count(case when order_status = 'IN TRANSIT' then 1 end)               as in_transit_orders,

    -- Fulfillment rate - delivered / total * 100
    round(count(case when order_status = 'DELIVERED' then 1 end)
          / nullif(count(order_id), 0) * 100, 2)                          as FULFILLMENT_RATE_PCT,

    -- Revenue KPIs
    round(sum(total_amount), 2)                                           as total_revenue,
    round(avg(total_amount), 2)                                           as avg_order_value,

    -- Payment breakdown
    count(case when payment_status = 'PAID' then 1 end)                   as paid_orders,
    count(case when payment_status = 'PENDING' then 1 end)                as payment_pending_orders,
    count(case when payment_status = 'OVERDUE' then 1 end)                as overdue_orders
from gold_schema.agg_base
group by customer_region,
         customer_segment;



create or replace dynamic table gold_schema.agg_supplier_performance
    target_lag = downstream
    warehouse  = flowbridge_pipline_wh
    comment    = 'Supplier performance KPIs. DOWNSTREAM from AGG_BASE.'
as
select
    supplier_id,
    supplier_name,
    supplier_country,

    -- Order volume
    count(order_id)                          as total_orders,

    -- Performance metrics
    round(avg(performance_score), 2)         as avg_performance_score,
    round(avg(lead_time_days), 2)            as avg_lead_time_days,

    -- Revenue
    round(sum(total_amount), 2)              as total_revenue,
    round(avg(total_amount), 2)              as avg_order_value,

    -- Delay metrics
    count(case when delay_days = 0 then 1 end)          as on_time_orders,
    count(case when delay_days > 0 then 1 end)          as delayed_orders,

    -- On-time rate
    round(count(case when delay_days = 0 then 1 end)
          / nullif(count(order_id), 0) * 100, 2)        as on_time_rate_pct,

    -- Average delay when delayed
    round(avg(case when delay_days > 0 then delay_days end), 2)      as avg_delay_days
from gold_schema.agg_base
group by
    supplier_id,
    supplier_name,
    supplier_country;



create or replace dynamic table gold_schema.agg_inventory_turnover
    target_lag = downstream
    warehouse  = flowbridge_pipline_wh
    comment    = 'Inventory turnover KPIs by warehouse and category. DOWNSTREAM from AGG_BASE.'
as
select
    warehouse_id,
    warehouse_location,
    category,

    -- Order volume
    count(order_id)                               as total_orders,
    sum(quantity)                                 as total_quantity_ordered,

    -- Inventory metrics
    round(avg(inventory_level), 2)                as avg_inventory_level,
    min(inventory_level)                          as min_inventory_level,
    max(inventory_level)                          as max_inventory_level,

    -- Revenue
    round(sum(total_amount), 2)                   as total_revenue,

    -- Inventory turnover ratio (higher = faster moving inventory)
    round(sum(quantity) / nullif(avg(inventory_level), 0), 2) as inventory_turnover_ratio
from gold_schema.agg_base
group by
    warehouse_id,
    warehouse_location,
    category;



CREATE OR REPLACE DYNAMIC TABLE GOLD_SCHEMA.AGG_SHIPMENT_DELAYS
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE  = FLOWBRIDGE_PIPLINE_WH
    COMMENT    = 'Shipment delay KPIs by carrier. DOWNSTREAM from AGG_BASE. Only shipped orders included.'
AS
SELECT
    CARRIER,
    CUSTOMER_REGION,

    -- Shipment counts
    COUNT(ORDER_ID)                                          AS TOTAL_SHIPMENTS,
    COUNT(CASE WHEN DELAY_DAYS = 0 THEN 1 END)               AS ON_TIME_SHIPMENTS,
    COUNT(CASE WHEN DELAY_DAYS > 0 THEN 1 END)               AS DELAYED_SHIPMENTS,

    -- On-time rate
    ROUND(
        COUNT(CASE WHEN DELAY_DAYS = 0 THEN 1 END)
        / NULLIF(COUNT(ORDER_ID), 0) * 100,
        2
    )                                                        AS ON_TIME_RATE_PCT,

    -- Delay metrics
    ROUND(AVG(DELAY_DAYS), 2)                                AS AVG_DELAY_DAYS,
    MAX(DELAY_DAYS)                                          AS MAX_DELAY_DAYS,

    -- Revenue
    ROUND(SUM(TOTAL_AMOUNT), 2)                              AS TOTAL_REVENUE
FROM GOLD_SCHEMA.AGG_BASE
-- Only include shipped orders
-- NULL SHIPMENT_ID = order not yet shipped
WHERE SHIPMENT_ID IS NOT NULL
GROUP BY
    CARRIER,
    CUSTOMER_REGION;


show dynamic tables in schema gold_schema;


select * from gold_schema.agg_order_fulfillment;
select * from gold_schema.agg_supplier_performance;
select * from gold_schema.agg_inventory_turnover;
select * from gold_schema.agg_shipment_delays;


select *
from table(information_schema.dynamic_table_refresh_history())
where schema_name = 'GOLD_SCHEMA'
order by refresh_start_time desc
limit 10;