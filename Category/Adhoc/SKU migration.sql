use role RL_PROD_MARKETING_ANALYTICS;
use warehouse ADW_2XLARGE_ADHOC_WH;
use database CUSTOMER_ANALYTICS;
use schema PRODUCT_REPORTING;


Create or replace temp table categories as
Select a.*
           from
EDWS_PROD.PROD_CMT_PRESENTATION.vw_sku a
    where MANAGER_ID in (
6305,
6273,
6291,
6309,
6339,
6360,
6488,
6296,
6324,
6443,
6306
)
or
(
SUB_CATEGORY IN (
628,
548,
343
));

select count(*) from categories;
--274747
--274747


/*FLORAL*/
-- select count(sku) from floral; -- 9312 - 9312

    Create or replace temp table Floral as
select * from categories
where MANAGER_ID = 6296
and
(BUYER_ID in (
6235,
6258)
and SUB_CATEGORY in (898,901,903,900)
or
(SKU_DESC like LOWER('%FLOWERS%')
or
SKU_DESC like UPPER('%FLOWERS%')
or
SKU_DESC like INITCAP('%FLOWERS%')
or
buyer_resp like LOWER('%FLOWERS%')
or
buyer_resp like UPPER('%FLOWERS%')
or
buyer_resp like INITCAP('%FLOWERS%')
or
sub_category_name like LOWER('%FLOWERS%')
or
sub_category_name like UPPER('%FLOWERS%')
or
sub_category_name like INITCAP('%FLOWERS%')))
;

/*Convenience*/
-- select count(sku) from Convenience; -- 179 - 179

Create or replace temp table Convenience as
Select * from categories
where MANAGER_ID = 6296
and
(RIGHT(sku_desc,2) like '-C'
or
RIGHT(sku_desc,2) like '-c'
or
RIGHT(sku_desc,2) like ' C'
or
RIGHT(sku_desc,2) like ' c'
or
SKU_DESC like '%Conv%'
or
SKU_DESC like '%CONV%'
or
SKU_DESC like '%conv%'
or
SKU_DESC like '%CNV%'
or
SKU_DESC like '%cnv%'
or
SKU_DESC like '%Cnv%'
or
SKU_DESC like '%(C)%'
or
SKU_DESC like '%(c)%'
);


/*LOOSE*/
-- select count(sku) from loose; -- 293 - 293
Create or replace temp table loose as
    Select * from categories
where MANAGER_ID = 6296
and
(
SKU_DESC like '%loose%'
or
SKU_DESC like '%Loose%'
or
(SKU_DESC) like '%LOOSE%'
);

/*SEASONAL*/
-- select count(sku) from seasonal; -- 19276- 20,234
Create or replace temp table seasonal as
select * from categories where
SEASONAL_PRODUCT_TYPE='S'
or
SEASONAL_PRODUCT_TYPE='F'
or
SEASONAL_PRODUCT_TYPE='X'
or
SEASONAL_PRODUCT_TYPE='E'
or
SKU_DESC like LOWER('%HALLOWEEN%')
or
SKU_DESC like UPPER('%HALLOWEEN%')
or
SKU_DESC like INITCAP('%HALLOWEEN%')
or
buyer_resp like LOWER('%HALLOWEEN%')
or
buyer_resp like UPPER('%HALLOWEEN%')
or
buyer_resp like INITCAP('%HALLOWEEN%')
or
sub_category_name like LOWER('%HALLOWEEN%')
or
sub_category_name like UPPER('%HALLOWEEN%')
or
sub_category_name like INITCAP('%HALLOWEEN%')
or
SKU_DESC like LOWER('%XMAS%')
or
SKU_DESC like UPPER('%XMAS%')
or
SKU_DESC like INITCAP('%XMAS%')
or
buyer_resp like LOWER('%XMAS%')
or
buyer_resp like UPPER('%XMAS%')
or
buyer_resp like INITCAP('%XMAS%')
or
sub_category_name like LOWER('%XMAS%')
or
SKU_DESC like LOWER('%CHRISTMAS%')
or
SKU_DESC like UPPER('%CHRISTMAS%')
or
SKU_DESC like INITCAP('%CHRISTMAS%')
or
buyer_resp like LOWER('%CHRISTMAS%')
or
buyer_resp like UPPER('%CHRISTMAS%')
or
buyer_resp like INITCAP('%CHRISTMAS%')
or
sub_category_name like LOWER('%CHRISTMAS%')
or
sub_category_name like UPPER('%CHRISTMAS%')
or
sub_category_name like INITCAP('%CHRISTMAS%')
or
sub_category_name like UPPER('%XMAS%')
or
sub_category_name like INITCAP('%XMAS%')
or
SKU_DESC like LOWER('%MINCE PIES%')
or
SKU_DESC like UPPER('%MINCE PIES%')
or
SKU_DESC like INITCAP('%MINCE PIES%')
or
buyer_resp like LOWER('%MINCE PIES%')
or
buyer_resp like UPPER('%MINCE PIES%')
or
buyer_resp like INITCAP('%MINCE PIES%')
or
sub_category_name like LOWER('%MINCE PIES%')
or
sub_category_name like UPPER('%MINCE PIES%')
or
sub_category_name like INITCAP('%MINCE PIES%')
or
SKU_DESC like LOWER('%HOT CROSS BUNS%')
or
SKU_DESC like UPPER('%HOT CROSS BUNS%')
or
SKU_DESC like INITCAP('%HOT CROSS BUNS%')
or
buyer_resp like LOWER('%HOT CROSS BUNS%')
or
buyer_resp like UPPER('%HOT CROSS BUNS%')
or
buyer_resp like INITCAP('%HOT CROSS BUNS%')
or
sub_category_name like LOWER('%HOT CROSS BUNS%')
or
sub_category_name like UPPER('%HOT CROSS BUNS%')
or
sub_category_name like INITCAP('%HOT CROSS BUNS%')
or
SKU_DESC like LOWER('%EASTER%')
or
SKU_DESC like UPPER('%EASTER%')
or
SKU_DESC like INITCAP('%EASTER%')
or
buyer_resp like LOWER('%EASTER%')
or
buyer_resp like UPPER('%EASTER%')
or
buyer_resp like INITCAP('%EASTER%')
or
sub_category_name like LOWER('%EASTER%')
or
sub_category_name like UPPER('%EASTER%')
or
sub_category_name like INITCAP('%EASTER%')
or
SKU_DESC like LOWER('%GINGERBREAD BUNNY%')
or
SKU_DESC like UPPER('%GINGERBREAD BUNNY%')
or
SKU_DESC like INITCAP('%GINGERBREAD BUNNY%')
or
buyer_resp like LOWER('%GINGERBREAD BUNNY%')
or
buyer_resp like UPPER('%GINGERBREAD BUNNY%')
or
buyer_resp like INITCAP('%GINGERBREAD BUNNY%')
or
sub_category_name like LOWER('%GINGERBREAD BUNNY%')
or
sub_category_name like UPPER('%GINGERBREAD BUNNY%')
or
sub_category_name like INITCAP('%GINGERBREAD BUNNY%')
or
BUYER_RESP like LOWER('%SEASONAL%')
or
BUYER_RESP like LOWER('%SEASONAL%')
or
BUYER_ID in (5021,5066,4991);

/*MATCHES*/
-- select count (sku) from matches; -- 2562 -- 2562
Create or replace temp table Matches as
Select * from categories
where SUB_CATEGORY in  (375,373);


 /*TARGET SKUS*/
-- select count(sku) from target_skus; -- 35128 -35128
--     select * from EDWS_PROD.PROD_CMT_PRESENTATION.VW_SKU_SUMMARY limit 5;
    Create or replace temp table target_skus AS
select a.* from categories a
         left join EDWS_PROD.PROD_CMT_PRESENTATION.VW_SKU_SUMMARY b
         on a.SKU= b.SKU
    where ((b.MARGIN_4WEEKS>=0.20
and
b.AVG_SELLING_PRICE_4WEEKS>=1)
or
(b.MARGIN_12WEEKS>=0.20
and
b.AVG_SELLING_PRICE_12WEEKS>=1)
or
(b.MARGIN_26WEEKS>=0.20
and
b.AVG_SELLING_PRICE_26WEEKS>=1))
and
a.END_DATE is NULL;


/*COUNTERS*/
-- select count (sku) from counters; -- 15007 - 15007
Create or replace temp table counters as
Select * from categories
where SUB_CATEGORY in
(
40,
220,
242,
244,
245,
425,
426,
428,
430,
456,
457,
931,
958,
483,
813,
113,
148,
914,
924,
118,
556,
114,
123,
124,
928,
951,
119,
462,
906,
964,
981,
414,
751,
618,
631,
650,
996,
994,
993,
627,
180,
184,
312,
313,
325,
350,
353,
856,
867,
869,
910,
919,
931,
932,
958,
912,
922,
925,
926,
929,
314,
315,
319,
438,
533,
757,
930,
943,
502,
503
);

/*NECTAR EXCLUSIONS*/
-- select count(sku) from nectar_exclusions; -- 3936 -- 1296
create or replace temp table nectar_exclusions as
Select a.sku from EDWS_PROD.PROD_CMT_PRESENTATION.VW_NECTAR_EXCLUSIONS a
inner join categories b on a.sku = b.sku;

/*BRANDED*/
-- select count(sku) from branded; -- 249503 -- 249503
Create or replace temp table branded as
    select * from categories
where brand is null;

/*SUSHI*/
-- select count(sku) from sushi; -- 0 - 0
Create or replace temp table sushi as
    select * from categories
where BUYER_ID=6237;

/*CAFE*/
-- select count(sku) from cafe; -- 10776 -- 10776
Create or replace temp table cafe as
  select * from categories
      where BUYER_ID=6438
OR

(SUB_CATEGORY in
(
115,
117,
121,
122,
226,
243,
305,
306,
307,
308,
309,
412,
535,
578,
704,
710,
829,
875,
907 )
);

/*OTHER SKUS*/
-- select count(sku) from other_skus;--1858 -- 1858
create or replace temp table other_skus as
    select * from categories
where SKU in (6439419,7548808)

OR

(SUB_CATEGORY in
(
395,
412,
893,
663,
812,
866)
);


/*PAIN MEDS*/
-- select count(SKU) from pain_meds; -- 366 -- 366
create or replace temp table pain_meds as
    select * from categories
             where SUB_CATEGORY_KEY =468;


/*FUTURE FORMAT SKUS*/
-- select count(sku) from future_format; -- 80 -- 80
Create or replace temp table future_format as
    select a.*,
           b.store
    from categories a
inner join EDWS_PROD.PROD_CMT_CAMPAIGN_01.USR_FF_SKUS_131015 b
    on a.sku=b.sku
    where b.store IS NULL;



create or replace temp table exclusions as
    select distinct SKU from Floral
union select sku from Convenience
union select sku from loose
union select sku from seasonal
union select sku from Matches
union select sku from counters
union select sku from nectar_exclusions
union select sku from branded
union select sku from sushi
union select sku from cafe
union select sku from other_skus
union select sku from pain_meds
union select sku from future_format;

select count (sku) from exclusions;

/*REMOVE ALL BUT TARGET SKUS*/
Create or replace temp table categories_2 as
    select *,
           case when JS_OWNLABEL_IND='N' then 'N'
           when SUB_BRAND is NULL then 'N' else 'Y'
    end as Own_Brand
    from categories
where sku in (select sku from target_skus)
and sku not in (select sku from floral)
and sku not in (select sku from Convenience)
and sku not in (select sku from loose)
and sku not in (select sku from seasonal)
and sku not in (select sku from Matches)
and sku not in (select sku from counters)
and sku not in (select sku from nectar_exclusions)
and sku not in (select sku from branded)
and sku not in (select sku from sushi)
and sku not in (select sku from cafe)
and sku not in (select sku from other_skus)
and sku not in (select sku from pain_meds)
and sku not in (select sku from future_format);

create or replace temp table target_minus_exclusions as
    select * from target_skus where sku not in (select sku from exclusions);

select count (distinct sku) from target_minus_exclusions;




select count (sku) from categories_2; ; -- 2714
select count (SKU) from target_skus;

--Own_Brand - 2720
--END_DATE is null - 2724
--AVG_SELLING_PRICE_26WEEKS> 1 - 2661
--NUM_NECT_CUSTOMERS_26WEEKS >= 5000 - 1891
--SALES_TURNOVER_4WEEKS >= 1000 - 2040
--MARGIN_26WEEKS >= 0.33 - 1937


-- add criteria
create or replace temp table categories_3 as
    select a.*,
           b.AVG_SELLING_PRICE_26WEEKS,
           b.NUM_NECT_CUSTOMERS_26WEEKS,
           b.SALES_TURNOVER_4WEEKS,
           b.MARGIN_26WEEKS
           from categories_2 a
             left join EDWS_PROD.PROD_CMT_PRESENTATION.VW_SKU_SUMMARY b
             on a.SKU= b.SKU
where
(Own_Brand= 'Y'
and
(a.END_DATE IS NULL
and b.AVG_SELLING_PRICE_26WEEKS> 1
and b.NUM_NECT_CUSTOMERS_26WEEKS >= 5000
and b.SALES_TURNOVER_4WEEKS >= 1000
and b.MARGIN_26WEEKS >= 0.33));



create or replace temp table categories_3 as
    select a.*,
           b.AVG_SELLING_PRICE_26WEEKS,
           b.NUM_NECT_CUSTOMERS_26WEEKS,
           b.SALES_TURNOVER_4WEEKS,
           b.MARGIN_26WEEKS
           from categories_2 a
             left join EDWS_PROD.PROD_CMT_PRESENTATION.VW_SKU_SUMMARY b
             on a.SKU= b.SKU
where
(Own_Brand= 'Y'
and
(a.END_DATE IS NULL
and  VW_SKU.DIM_SKU_SUMMARY.AVG_SELLING_PRICE_26WEEKS> 1
and b.NUM_NECT_CUSTOMERS_26WEEKS >= 5000
and b.SALES_TURNOVER_4WEEKS >= 1000
and b.MARGIN_26WEEKS >= 0.33));



select count (*) from categories_2; -- 2729
select count (*) from categories_3; -- 1248 -- 2623 -- 1900 -- 2034 -- 1978

-- add on HFSS and points
create or replace temp table categories_4 as
    select *,
           case when sku in (select item_cd from EDWS_PROD.PROD_CMT_PRESENTATION.VW_DIM_HFSS_ITEMS) then 'Y'
    else 'N'
    end as HFSS_FLAG,
    case when (manager_id in (5264) or manager_id in (5102)) then (ROUND((AVG_SELLING_PRICE_26WEEKS*0.15)/0.05)*0.05)*200
    when manager_id in (5245) then (ROUND((AVG_SELLING_PRICE_26WEEKS*0.12)/0.05)*0.05)*200
    else (ROUND((AVG_SELLING_PRICE_26WEEKS*0.2)/0.05)*0.05)*200
    end as points
           from categories_3;

-- draw out only required columns
// Export and send to comms - this will be for their first tab //
create or replace temp table First_tab as
    select SKU,
    SKU_DESC,
    BUYER_RESP,
    MANAGER_RESP,
    SKU_KEY,
    SUB_CATEGORY_NAME,
    POINTS,
    OWN_BRAND,
    HFSS_FLAG
    from categories_4;

select * from First_tab;


/*EANs*/
-- checking if the SKUs provided are distinct
create or replace temp table SKU_distinct as
    select distinct sku,
                    sku_key
    from First_tab;

select count(sku), count(distinct sku) from SKU_distinct;


-- joining uploaded table onto various sku tables to check validity and to attach EAN
create or replace temp table EAN_SKU_LIST as
    select distinct esl.sku,
                    sm.sku_key,
                    en.ean,
                    en.primary_upc_ean_ind
    from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_EAN en

    inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_SKU_MAP sm
    on en.sku_key = sm.sku_key

    inner join sku_distinct esl
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

select count(distinct sku) from EAN_SKU_LIST;


// EXPORT and send to comms - this will be for their second tab //
create or replace temp table Second_Tab as
    select
           SKU,
           SKU_KEY,
           EAN,
           PRIMARY_UPC_EAN_IND
from EAN_SKU_LIST;

select * from Second_Tab;