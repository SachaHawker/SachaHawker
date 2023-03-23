use role RL_PROD_MARKETING_ANALYTICS;
use warehouse ADW_2XLARGE_ADHOC_WH;
use database CUSTOMER_ANALYTICS;
use schema PRODUCT_REPORTING;

-- select distinct sub_category, sub_category_name from EDWS_PROD.PROD_CMT_PRESENTATION.vw_sku where category = 918;


create or replace table XC8_mincepie_redemptions as
select * from all_redemptions where sku = 226;

create or replace table XC9_PP_redemptions as
select * from all_redemptions where sku=230;

    select * from ALL_REDEMPTIONS where sku = '226' -- 3967

-- mincepie transactions from redeemers of the campaign
create or replace temp table mincepie_trx as
    select   stl.party_account_id,
           stl.transaction_date,
           stl.transaction_time,
          pay.payment_value,
          sku.sku,
          san.sku as red_sku
          , sum(stl.extended_price) as sales
            , sum(stl.item_quantity) as quantity
	from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_shopping_transaction_line as stl
	    	inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_ean as ean
		on stl.ean_key = ean.ean_key
	inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_c_sku_map as sku
		on ean.sku_key = sku.sku_key
	inner join XC8_mincepie_redemptions as san on stl.party_account_id = san.sr_id
    inner join  EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_payment_line pay on stl.party_account_id = pay.party_account_id
        where stl.transaction_date >= '2022-12-07'
        and stl.transaction_date <= '2023-01-17'
    	and stl.extended_price>0 /*transaction value bigger than zero*/
		and stl.party_account_type_code in ('04') /*in store only for threshold*/
        and sku.sku in (111652,
7806596,
6623618,
8074214,
6033576,
7324417,
7978504,
6635438,
8074270,
8074255,
8121643)
	group by 1,2,3,4,5,6;

select distinct red_sku from mincepie_trx;
select * from MINCEPIE_TRX;

-- avg spend on mince pies
select
     avg(sales/quantity) as avg_spd
from MINCEPIE_TRX;
---1.66

-- min/max spend on mince pies
select
     min(sales/quantity) as avg_spd
from MINCEPIE_TRX;
--22p

select
     max(sales/quantity) as avg_spd
from MINCEPIE_TRX;
--£2.5

create or replace temp table avg_spend as
    select (sales/quantity) as avg_spd
from mincepie_trx;


-- avg spend on mince pies
create or replace temp table mince_pie_avg_spend as
    select SUM_EXTENDED_PRICE/NUM_TRANSACTIONS as avg_spd
from MINCE_PIES_TRX;


--avg spend percentiles
select percentile_cont(0.25) within group (order by AVG_SPD)  as avg_spend_p25
     , percentile_cont(0.5) within group (order by AVG_SPD)  as avg_spend_p50
     , percentile_cont(0.75) within group (order by AVG_SPD)  as avg_spend_p75
from avg_spend ;
-- 25: 1.50
-- 50: 1.75
-- 75: 1.75






-- PP transactions from redeemers of the campaign
create or replace temp table PP_trx as
    select   stl.party_account_id,
           stl.transaction_date,
           stl.transaction_time,
          pay.payment_value,
          sku.sku,
          san.sku as red_sku
          , sum(stl.extended_price) as sales
            , sum(stl.item_quantity) as quantity
	from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_shopping_transaction_line as stl
	    	inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_ean as ean
		on stl.ean_key = ean.ean_key
	inner join EDWS_PROD.PROD_CMT_PRESENTATION.vw_sku as sku
		on ean.sku_key = sku.sku_key
	inner join XC9_PP_redemptions as san on stl.party_account_id = san.sr_id
    inner join  EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_payment_line pay on stl.party_account_id = pay.party_account_id
        where stl.transaction_date >= '2023-02-14'
        and stl.transaction_date <= '2023-02-28'
    	and stl.extended_price>0 /*transaction value bigger than zero*/
		and stl.party_account_type_code in ('04') /*in store only for threshold*/
        and sku.sub_category in (960)
	group by 1,2,3,4,5,6;

select * from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_c_sku_map limit 5;
select * from EDWS_PROD.PROD_CMT_PRESENTATION.vw_sku limit 5;

select * from XC9_PP_redemptions limit 5;

select distinct red_sku from PP_trx;
select * from PP_TRX;

-- avg spend on PP
select
     avg(sales/quantity) as avg_spd
from PP_TRX;
--- 2.24

-- min/max spend on mince pies
select
     min(sales/quantity) as avg_spd
from PP_TRX;
--10p

select
     max(sales/quantity) as avg_spd
from PP_TRX;
--6.5

create or replace temp table avg_spend_PP as
    select (sales/quantity) as avg_spd
from PP_trx;



--avg spend percentiles
select percentile_cont(0.25) within group (order by AVG_SPD)  as avg_spend_p25
     , percentile_cont(0.5) within group (order by AVG_SPD)  as avg_spend_p50
     , percentile_cont(0.75) within group (order by AVG_SPD)  as avg_spend_p75
from avg_spend_PP ;
-- 25:1.8
-- 50:2.25
-- 75:2.6
