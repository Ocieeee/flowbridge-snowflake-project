-- =============================================================================
-- FLOWBRIDGE - Silver (stg_orders) to Star schema
-- =============================================================================
use role sysadmin;
use warehouse flowbridge_pipline_wh;
use database flowbridge_dev_db;
use schema silver_schema;

-- 1. Sequences
create sequence if not exists silver_schema.dim_customer_sk_seq  start = 1 increment = 1;
create sequence if not exists silver_schema.dim_product_sk_seq   start = 1 increment = 1;
create sequence if not exists silver_schema.dim_supplier_sk_seq  start = 1 increment = 1;
create sequence if not exists silver_schema.dim_warehouse_sk_seq start = 1 increment = 1;
create sequence if not exists silver_schema.dim_shipment_sk_seq  start = 1 increment = 1;
create sequence if not exists silver_schema.fact_orders_sk_seq   start = 1 increment = 1;

-- 2. Dimension tables
create or replace table silver_schema.dim_customer (
    customer_sk       number default silver_schema.dim_customer_sk_seq.nextval,
    customer_id       string not null,
    customer_name     string not null,
    customer_region   string not null,
    customer_segment  string not null,
    is_active         boolean default true,
    created_at        timestamp_ntz default current_timestamp(),
    updated_at        timestamp_ntz default current_timestamp()
) comment = 'Customer dimension - surrogate key via sequence';

create or replace table silver_schema.dim_product (
    product_sk        number default silver_schema.dim_product_sk_seq.nextval,
    product_id        string not null,
    product_name      string not null,
    category          string not null,
    unit_price        float  not null,
    is_active         boolean default true,
    created_at        timestamp_ntz default current_timestamp(),
    updated_at        timestamp_ntz default current_timestamp()
) comment = 'Product dimension - surrogate key via sequence';

create or replace table silver_schema.dim_supplier (
    supplier_sk       number default silver_schema.dim_supplier_sk_seq.nextval,
    supplier_id       string not null,
    supplier_name     string not null,
    supplier_country  string not null,
    lead_time_days    number not null,
    performance_score float  not null,
    is_active         boolean default true,
    created_at        timestamp_ntz default current_timestamp(),
    updated_at        timestamp_ntz default current_timestamp()
) comment = 'Supplier dimension - surrogate key via sequence';

create or replace table silver_schema.dim_warehouse (
    warehouse_sk       number default silver_schema.dim_warehouse_sk_seq.nextval,
    warehouse_id       string not null,
    warehouse_location string not null,
    is_active          boolean default true,
    created_at         timestamp_ntz default current_timestamp(),
    updated_at         timestamp_ntz default current_timestamp()
) comment = 'Warehouse dimension - surrogate key via sequence';

create or replace table silver_schema.dim_shipment (
    shipment_sk         number default silver_schema.dim_shipment_sk_seq.nextval,
    shipment_id         string not null,
    carrier             string not null,
    ship_date           timestamp_ntz,
    estimated_delivery  date,
    is_active           boolean default true,
    created_at          timestamp_ntz default current_timestamp(),
    updated_at          timestamp_ntz default current_timestamp()
) comment = 'Shipment dimension - surrogate key via sequence';

-- 3. Fact table
create or replace table silver_schema.fact_orders (
    order_sk         number default silver_schema.fact_orders_sk_seq.nextval,
    order_id         string not null,
    customer_sk      number not null,
    product_sk       number not null,
    supplier_sk      number not null,
    warehouse_sk     number not null,
    shipment_sk      number not null,
    order_date       timestamp_ntz not null,
    order_status     string not null,
    payment_status   string not null,
    quantity         number not null,
    unit_price       float  not null,
    total_amount     float  not null,
    delay_days       number not null,
    inventory_level  number not null,
    ingested_at      timestamp_ntz,
    created_at       timestamp_ntz default current_timestamp(),
    updated_at       timestamp_ntz default current_timestamp()
) comment = 'Fact orders - 5 dimension surrogate keys and measures';

-- 4. Stream on stg_orders
create or replace stream silver_schema.stg_orders_stream
    on table silver_schema.stg_orders
    show_initial_rows = true
    comment = 'Stream on stg_orders';

-- 5. Procedure
create or replace procedure silver_schema.silver_to_star()
    returns string
    language sql
    comment = 'Loads dimensions and fact from new stg_orders rows'
as
$$
declare
    v_rows number default 0;
begin
    -- Part 1: snapshot new rows from the stg_orders stream
    create or replace temporary table silver_schema.tmp_stream_data as
    select *
    from silver_schema.stg_orders_stream
    where metadata$action = 'INSERT';

    select count(*) into :v_rows from silver_schema.tmp_stream_data;

    if (v_rows = 0) then
        drop table if exists silver_schema.tmp_stream_data;
        return 'No new rows in stg_orders_stream. Nothing to process.';
    end if;

    -- Part 2: dim_customer
    merge into silver_schema.dim_customer as tgt
    using (
        select
            upper(customer_id)      as customer_id,
            upper(customer_name)    as customer_name,
            upper(customer_region)  as customer_region,
            upper(customer_segment) as customer_segment
        from silver_schema.tmp_stream_data
        qualify row_number() over (partition by upper(customer_id) order by order_date desc) = 1
    ) as src
    on tgt.customer_id = src.customer_id
    when matched then update set
        tgt.customer_name    = src.customer_name,
        tgt.customer_region  = src.customer_region,
        tgt.customer_segment = src.customer_segment,
        tgt.is_active        = true,
        tgt.updated_at       = current_timestamp()
    when not matched then insert (
        customer_id, customer_name, customer_region, customer_segment
    ) values (
        src.customer_id, src.customer_name, src.customer_region, src.customer_segment
    );

    -- Part 3: dim_product
    merge into silver_schema.dim_product as tgt
    using (
        select
            upper(product_id)   as product_id,
            upper(product_name) as product_name,
            upper(category)     as category,
            unit_price
        from silver_schema.tmp_stream_data
        qualify row_number() over (partition by upper(product_id) order by order_date desc) = 1
    ) as src
    on tgt.product_id = src.product_id
    when matched then update set
        tgt.product_name = src.product_name,
        tgt.category     = src.category,
        tgt.unit_price   = src.unit_price,
        tgt.is_active    = true,
        tgt.updated_at   = current_timestamp()
    when not matched then insert (
        product_id, product_name, category, unit_price
    ) values (
        src.product_id, src.product_name, src.category, src.unit_price
    );

    -- Part 4: dim_supplier
    merge into silver_schema.dim_supplier as tgt
    using (
        select
            upper(supplier_id)      as supplier_id,
            upper(supplier_name)    as supplier_name,
            upper(supplier_country) as supplier_country,
            lead_time_days,
            performance_score
        from silver_schema.tmp_stream_data
        qualify row_number() over (partition by upper(supplier_id) order by order_date desc) = 1
    ) as src
    on tgt.supplier_id = src.supplier_id
    when matched then update set
        tgt.supplier_name     = src.supplier_name,
        tgt.supplier_country  = src.supplier_country,
        tgt.lead_time_days    = src.lead_time_days,
        tgt.performance_score = src.performance_score,
        tgt.is_active         = true,
        tgt.updated_at        = current_timestamp()
    when not matched then insert (
        supplier_id, supplier_name, supplier_country, lead_time_days, performance_score
    ) values (
        src.supplier_id, src.supplier_name, src.supplier_country, src.lead_time_days, src.performance_score
    );

    -- Part 5: dim_warehouse
    merge into silver_schema.dim_warehouse as tgt
    using (
        select
            upper(warehouse_id)       as warehouse_id,
            upper(warehouse_location) as warehouse_location
        from silver_schema.tmp_stream_data
        qualify row_number() over (partition by upper(warehouse_id) order by order_date desc) = 1
    ) as src
    on tgt.warehouse_id = src.warehouse_id
    when matched then update set
        tgt.warehouse_location = src.warehouse_location,
        tgt.is_active          = true,
        tgt.updated_at         = current_timestamp()
    when not matched then insert (
        warehouse_id, warehouse_location
    ) values (
        src.warehouse_id, src.warehouse_location
    );

    -- Part 6: dim_shipment (skip placeholder 'UNKNOWN' shipments)
    merge into silver_schema.dim_shipment as tgt
    using (
        select
            upper(shipment_id) as shipment_id,
            upper(carrier)     as carrier,
            ship_date,
            estimated_delivery
        from silver_schema.tmp_stream_data
        where upper(shipment_id) != 'UNKNOWN'
        qualify row_number() over (partition by upper(shipment_id) order by order_date desc) = 1
    ) as src
    on tgt.shipment_id = src.shipment_id
    when matched then update set
        tgt.carrier            = src.carrier,
        tgt.ship_date          = src.ship_date,
        tgt.estimated_delivery = src.estimated_delivery,
        tgt.is_active          = true,
        tgt.updated_at         = current_timestamp()
    when not matched then insert (
        shipment_id, carrier, ship_date, estimated_delivery
    ) values (
        src.shipment_id, src.carrier, src.ship_date, src.estimated_delivery
    );

    -- Part 7: fact_orders
    merge into silver_schema.fact_orders as tgt
    using (
        select
            upper(s.order_id)             as order_id,
            dc.customer_sk,
            dp.product_sk,
            ds.supplier_sk,
            dw.warehouse_sk,
            coalesce(dsh.shipment_sk, -1) as shipment_sk,
            s.order_date,
            upper(s.order_status)         as order_status,
            upper(s.payment_status)       as payment_status,
            s.quantity,
            s.unit_price,
            s.total_amount,
            s.delay_days,
            s.inventory_level,
            s.ingested_at
        from silver_schema.tmp_stream_data s
        inner join silver_schema.dim_customer  dc  on dc.customer_id  = upper(s.customer_id)
        inner join silver_schema.dim_product   dp  on dp.product_id   = upper(s.product_id)
        inner join silver_schema.dim_supplier  ds  on ds.supplier_id  = upper(s.supplier_id)
        inner join silver_schema.dim_warehouse dw  on dw.warehouse_id = upper(s.warehouse_id)
        left  join silver_schema.dim_shipment  dsh on dsh.shipment_id = upper(s.shipment_id)
        qualify row_number() over (partition by upper(s.order_id) order by s.order_date desc) = 1
    ) as src
    on tgt.order_id = src.order_id
    when matched then update set
        tgt.customer_sk     = src.customer_sk,
        tgt.product_sk      = src.product_sk,
        tgt.supplier_sk     = src.supplier_sk,
        tgt.warehouse_sk    = src.warehouse_sk,
        tgt.shipment_sk     = src.shipment_sk,
        tgt.order_date      = src.order_date,
        tgt.order_status    = src.order_status,
        tgt.payment_status  = src.payment_status,
        tgt.quantity        = src.quantity,
        tgt.unit_price      = src.unit_price,
        tgt.total_amount    = src.total_amount,
        tgt.delay_days      = src.delay_days,
        tgt.inventory_level = src.inventory_level,
        tgt.ingested_at     = src.ingested_at,
        tgt.updated_at      = current_timestamp()
    when not matched then insert (
        order_id, customer_sk, product_sk, supplier_sk, warehouse_sk, shipment_sk,
        order_date, order_status, payment_status, quantity, unit_price, total_amount,
        delay_days, inventory_level, ingested_at
    ) values (
        src.order_id, src.customer_sk, src.product_sk, src.supplier_sk, src.warehouse_sk, src.shipment_sk,
        src.order_date, src.order_status, src.payment_status, src.quantity, src.unit_price, src.total_amount,
        src.delay_days, src.inventory_level, src.ingested_at
    );

    drop table if exists silver_schema.tmp_stream_data;

    return 'silver_to_star completed successfully | rows processed: ' || v_rows;

exception
    when other then
        drop table if exists silver_schema.tmp_stream_data;
        raise;
end;
$$;

-- 6. Task
create or replace task silver_schema.silver_to_star_task
    warehouse = FLOWBRIDGE_PIPLINE_WH
    schedule  = '1 minute'
when system$stream_has_data('FLOWBRIDGE_DEV_DB.SILVER_SCHEMA.STG_ORDERS_STREAM')
as
    call silver_schema.silver_to_star();

alter task silver_schema.silver_to_star_task resume;

show tasks;

-- 7. Checks
select system$stream_has_data('FLOWBRIDGE_DEV_DB.SILVER_SCHEMA.STG_ORDERS_STREAM');



select * from silver_schema.dim_customer

select * from silver_schema.dim_product

select * from silver_schema.dim_supplier

select * from silver_schema.dim_warehouse

select * from silver_schema.dim_shipment

select * from silver_schema.fact_orders