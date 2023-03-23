USE ROLE RL_PROD_MARKETING_ANALYTICS;
USE WAREHOUSE WHS_PROD_MARKETING_ANALYTICS_LARGE;
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA PRODUCT_REPORTING;

select * from C18 order by SKU asc;

create or replace temp table c18 as
SELECT DISTINCT
    SKU.SKU,
    EAN.EAN,
    SKU.SKU_DESC,
    SKU2.CATEGORY_NAME,
    SKU2.SUB_CATEGORY_NAME,
EAN.primary_upc_ean_ind
FROM EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_SKU_MAP SKU
INNER JOIN EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_EAN EAN
   ON SKU.SKU_KEY = EAN.SKU_KEY
inner join EDWS_PROD.PROD_CMT_PRESENTATION.vw_sku SKU2
on SKU.SKU_KEY = SKU2.SKU_KEY
WHERE EAN.EAN_END_DATE IS NULL
    AND SKU.SKU in (select sku from  SKU_C18 );

select * from SKU_C18;