CREATE DATABASE WOODCHUCK_JSON_CUSTOMER_ORDERS_DB;

USE DATABASE WOODCHUCK_JSON_CUSTOMER_ORDERS_DB;

CREATE FILE FORMAT _woodchuck_global_tools.file_formats.main_json_format
    TYPE = JSON;

CREATE OR REPLACE STAGE _woodchuck_global_tools.internal_stages.stage_json_prods
    FILE_FORMAT = _woodchuck_global_tools.file_formats.main_json_format;

SELECT *
FROM @_woodchuck_global_tools.internal_stages.stage_json_prods/customers_data.json;

LIST @_woodchuck_global_tools.internal_stages.stage_json_prods;

COMMENT ON STAGE _woodchuck_global_tools.internal_stages.stage_json_prods IS 'A stage for storing customers and orders;';

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

/*

Тука пиша код от упр-то


COPY INTO raw_orders_json(item)
    FROM @_woodchuck_global_tools.internal_stages.stage_json_prods/customers_data.json    
FILE_FORMAT = (FORMAT_NAME = '_woodchuck_global_tools.file_formats.main_json_format');

SELECT * FROM raw_orders_json;

TRUNCATE TABLE raw_orders_json;

CREATE SCHEMA _woodchuck_global_tools.tasks;



CREATE TASK WOODCHUCK_JSON_CUSTOMER_ORDERS_DB.PUBLIC.copy_customer_data_every_2min
    WAREHOUSE = WOODCHUCK__WH
    SCHEDULE = 'USING CRON 0/2 * * * * UTC'
AS
    BEGIN
        COPY INTO WOODCHUCK_JSON_CUSTOMER_ORDERS_DB.RAW_DATA.raw_orders_json(item)
        FROM            @_woodchuck_global_tools.internal_stages.stage_json_prods/customers_data.json    
        FILE_FORMAT = (FORMAT_NAME = '_woodchuck_global_tools.file_formats.main_json_format');

        TRUNCATE TABLE WOODCHUCK_JSON_CUSTOMER_ORDERS_DB.RAW_DATA.raw_orders_json;
    
    END;

USE SCHEMA WOODCHUCK_JSON_CUSTOMER_ORDERS_DB.PUBLIC;

SHOW TASKS;
EXECUTE TASK COPY_CUSTOMER_DATA_EVERY_2MIN;

EXECUTE TASK WOODCHUCK_JSON_CUSTOMER_ORDERS_DB.PUBLIC.copy_customer_data_every_2min;

        --ДАВАНЕ НА ПРАВО НА РОЛЯ

GRANT
    EXECUTE TASK
ON ACCOUNT
    TO ROLE RABBIT__ROLE;

        --СЪЗДАВАНЕ НА ПРОЦЕДУРА ЗА СЪЗДАВАНЕ НА ПРАВА 
        
CREATE PROCEDURE _cool_db_name.grant_task_execute_to_role(
ROLE_NAME VARCHAR
)
RETURNS VARCHAR   
    BEGIN

        -- GRANT
        --     EXECUTE TASK
        -- ON ACCOUNT
        --     TO ROLE IDENTIFIER(:ROLE_NAME);

    EXECUTE IMMEDIATE 'GRANT EXECUTE TASK ON ACCOUNT TO ROLE ' || :ROLE_NAME;
    
    END;

CALL _cool_db_name.grant_task_execute_to_role('WOODCHUK__ROLE');



*/

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
LATERAL FLATTEN(input=>item);

CREATE OR REPLACE TABLE td_orders AS 
SELECT 
    value:order_id::STRING as order_id,
    value:customer_id::STRING as customer_id,
    value:order_date::DATE as order_date,
    value:total_amount::NUMBER as total_amount,
    value:items::array as items,
    value:shipping_method::STRING as shipping_method
FROM WOODCHUCK_JSON_CUSTOMER_ORDERS_DB.RAW_DATA.raw_orders_json,
LATERAL FLATTEN(input=>item);

CREATE OR REPLACE TABLE td_order_items AS 
SELECT 
    value:product_id::STRING as product_id,
    value:name::STRING as name,
    value:quantity::NUMBER as quantity,
    value:price::NUMBER as price
FROM WOODCHUCK_JSON_CUSTOMER_ORDERS_DB.PARSED_DATA.td_orders,
LATERAL FLATTEN(input=>items);

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

TRUNCATE TABLE IF EXISTS raw_customers_json;
TRUNCATE TABLE IF EXISTS raw_orders_json;




