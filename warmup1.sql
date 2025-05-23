CREATE DATABASE WOODCHUCK_JSON_CUSTOMER_ORDERS_DB;

USE DATABASE WOODCHUCK_JSON_CUSTOMER_ORDERS_DB

CREATE FILE FORMAT _woodchuck_global_tools.file_formats.main_json_format
    TYPE = JSON

CREATE OR REPLACE STAGE _woodchuck_global_tools.internal_stages.stage_json_prods
    FILE_FORMAT = _woodchuck_global_tools.file_formats.main_json_format

LIST @_woodchuck_global_tools.internal_stages.stage_json_prods

COMMENT ON STAGE _woodchuck_global_tools.internal_stages.stage_json_prods IS 'A stage for storing customers and orders;'

-- 5. Създайте подходящи таблици за суровите данни:
-- raw_customers_json
-- raw_orders_json

CREATE SCHEMA WOODCHUCK_JSON_CUSTOMER_ORDERS_DB.RAW_DATA;
USE SCHEMA WOODCHUCK_JSON_CUSTOMER_ORDERS_DB.RAW_DATA;

CREATE OR REPLACE TEMP TABLE raw_customers_json(
    item array 
);

CREATE OR REPLACE TEMP TABLE raw_orders_json(
    item array
);


INSERT INTO raw_customers_json(item)
SELECT parse_json(*)
FROM @_woodchuck_global_tools.internal_stages.stage_json_prods/customers_data.json;

INSERT INTO raw_orders_json(item)
SELECT parse_json(*)
FROM @_woodchuck_global_tools.internal_stages.stage_json_prods/orders_data.json;

--6. Създайте подходящи таблици за същинските данни:
-- td_customers
-- td_orders
-- td_order_items
-- 7. Заредете данните от JSON файловете - в таблиците за сурови данни
-- 8. Извлечете данните от суровите таблици и ги разпределете в td_* таблиците

CREATE SCHEMA WOODCHUCK_JSON_CUSTOMER_ORDERS_DB.PARSED_DATA;
USE SCHEMA WOODCHUCK_JSON_CUSTOMER_ORDERS_DB.PARSED_DATA;

CREATE OR REPLACE TABLE td_customers AS 
SELECT 
    value:customer_id::STRING as customer_id,
    value:name::STRING as name,
    value:email::STRING as email,
    value:registration_date::DATE as registration_date,
    value:address::ARRAY as address,
    value:loyalty_points::NUMBER as loyalty_points,
FROM WOODCHUCK_JSON_CUSTOMER_ORDERS_DB.RAW_DATA.raw_customers_json,
LATERAL FLATTEN(input=>item)

CREATE OR REPLACE TABLE td_orders AS 
SELECT 
    value:order_id::STRING as order_id,
    value:customer_id::STRING as customer_id,
    value:order_date::DATE as order_date,
    value:total_amount::NUMBER as total_amount,
    value:items::array as items,
    value:shipping_method::STRING as shipping_method
FROM WOODCHUCK_JSON_CUSTOMER_ORDERS_DB.RAW_DATA.raw_orders_json,
LATERAL FLATTEN(input=>item)

CREATE OR REPLACE TABLE td_order_items AS 
SELECT 
    value:product_id::STRING as product_id,
    value:name::STRING as name,
    value:quantity::NUMBER as quantity,
    value:price::NUMBER as price
FROM WOODCHUCK_JSON_CUSTOMER_ORDERS_DB.PARSED_DATA.td_orders,
LATERAL FLATTEN(input=>items)

-- 9. Създайте таблица, която да съдържа агрегирана информация от броя на потребителите, които са се регистрирали до момента в системата.
USE SCHEMA WOODCHUCK_JSON_CUSTOMERS_ORDERS_DB.PARSED_DATA;

CREATE OR REPLACE TABLE td_registered_users_count AS
SELECT COUNT(*)
FROM td_customers;

-- 10. Създайте таблица, която да агрегира общото количество продадени продукти и тяхната цена.
USE SCHEMA WOODCHUCK_JSON_CUSTOMERS_ORDERS_DB.PARSED_DATA;

CREATE OR REPLACE TABLE td_order_items_sum AS
SELECT count(*) as item_count, SUM(QUANTITY*PRICE) as total_amount
FROM td_order_items


-- 11. Изтриите СУРОВИТЕ таблици.
USE SCHEMA WOODCHUCK_JSON_CUSTOMER_ORDERS_DB.RAW_DATA;

DROP TABLE raw_customers_json;
DROP TABLE raw_orders_json;




