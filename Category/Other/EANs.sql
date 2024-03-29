USE ROLE RL_PROD_MARKETING_ANALYTICS;
USE WAREHOUSE WHS_PROD_MARKETING_ANALYTICS_LARGE;
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA Product_REPORTING;



select distinct manager_resp, SKU from EDWS_PROD.PROD_CMT_PRESENTATION.vw_sku
where sku in (948,
902,
911,
450,
50);

select distinct
--        b.sub_category_name,
--        a.sku,
--        a.sku_desc,
--        b.sub_category
a.manager_resp
       from  EDWS_PROD.PROD_CMT_PRESENTATION.vw_sku a
         inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_sub_category_map b
         on a.SUB_CATEGORY_KEY = b.SUB_CATEGORY_KEY
       where a.sku in ();

Select * from EDWS_PROD.PROD_CMT_PRESENTATION.vw_sku limit 5;
select * from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_SKU_MAP limit 5;
select * from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_EAN limit 5;
select * from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_sub_category_map limit 5;
select * from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_category_map limit 5;;
-- select * from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_posass_flat2;

create or replace temp table sku_list as
SELECT DISTINCT
    SKU.SKU,
    EAN.EAN,
    SKU.SKU_DESC,
    SUBCAT.SUB_CATEGORY_NAME,
    CAT.CATEGORY_NAME,
    EAN.primary_upc_EAN_ind
FROM EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_SKU_MAP SKU
INNER JOIN EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_EAN EAN
   ON SKU.SKU_KEY = EAN.SKU_KEY
INNER JOIN EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_sub_category_map SUBCAT
    ON SKU.SUB_CATEGORY_KEY = SUBCAT.SUB_CATEGORY_KEY
INNER JOIN EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_category_map CAT
    ON SKU.CATEGORY_KEY = CAT.CATEGORY_KEY
WHERE EAN.EAN_END_DATE IS NULL
    AND SKU.SKU in
      (114882,
114905,
172684,
388061,
1162615,
1162622,
1186505,
1242942,
1269499,
1327267,
6008871,
6018471,
6286566,
6296734,
6296738,
6296740,
6601783,
7220800,
7444159,
7620643,
7645618,
7645632,
7649054,
7689244,
7845866,
7853727,
7889884,
7946678,
8034939,
8037816,
8049853,
8101497,
8124023,
8140501,
8140504,
8143997,
8144684,
8144686,
8145072,
8145403,
8145870,
6528180,
8042588

     )
;

Select * from SKU_list
Order by SKU;


