/**********************************************************************************************************************
Project:        SKU to EAN
Requester:      Product CRM Team - Transition to EDWS (in SQL)
Name:           Yunus Malik
Date:           05 June 2020
Background:
As part of Sainsbury's and Campaign Analytics' move away from SAS, we are transitioning everything from SAS to raw
SQL (EDW ---> EDWS)
Objective:
Move code over to EDWS and written in full SQL
Change History:
Name:           Date:           Note:
Yunus Malik     05JUN2020       Setup repository and project
Yunus Malik     08JUN2020       Transitioned code over
**********************************************************************************************************************/

 -- import your initial sku list using the Snowflake wizard into the PRODUCT_REPORTING and assign the table name below

use role RL_PROD_MARKETING_ANALYTICS;
use warehouse WHS_PROD_MARKETING_ANALYTICS_X2LARGE;
use database CUSTOMER_ANALYTICS;
use schema PRODUCT_REPORTING;

-- update initials and table name
set initials = 'SH';
set tbl = 'SH_X03232_SKU_list'; -- put the table name you gave your CSV file here
set tbl_name = $initials||'_'||$tbl;

Set tbl = '"CUSTOMER_ANALYTICS"."PRODUCT_REPORTING"."SH_X03232_SKU_list"';

select * from "CUSTOMER_ANALYTICS"."PRODUCT_REPORTING"."SH_X03232_SKU_list";
select * from identifier ($tbl);

-- checking if the SKUs provided are distinct
create or replace temp table identifier($tbl_name) as
    select distinct sku,
                    sku_key
    from table ($tbl);

select * from table ($tbl_name);

-- joining uploaded table onto various sku tables to check validity and to attach EAN
create or replace temp table EAN_SKU_LIST as
    select distinct esl.sku,
                    sm.sku_key,
                    en.ean,
                    en.primary_upc_ean_ind
    from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_EAN en

    inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_SKU_MAP sm
    on en.sku_key = sm.sku_key

    inner join table ($tbl_name) esl
    on sm.sku = esl.sku

    inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_C_SUB_CATEGORY_MAP scm
    on scm.sub_category_key = sm.sub_category_key

    inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_C_SEGMENT_MAP sgmt
    on sgmt.segment_key = sm.segment_key

    left join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_UDA_ITEM_LOV lv
    on lv.sku_key = sm.sku_key
        and lv.uda_id = 7
        and lv.end_date is null

    left join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_UDA_VALUE uv
    on uv.uda_value = lv.uda_value
        and uv.uda_id = 7
        and uv.end_date is null

    where sm.end_date is null;

select * from EAN_SKU_LIST;

create or replace table XC3_EAN_SKU as select * from EAN_SKU_LIST;
select * from XC3_EAN_SKU;


-- next we need to export back out to CSV, use the download button near the previewed data