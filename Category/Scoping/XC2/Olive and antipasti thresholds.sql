
use role RL_PROD_MARKETING_ANALYTICS;
use database CUSTOMER_ANALYTICS;
use warehouse WHS_PROD_MARKETING_ANALYTICS_X2LARGE;
use schema PRODUCT_REPORTING;

-- ***************************************************************************************
-- CVU_HOUSEHOLD_SEGMENTATION


-- using the XC9 customer base
set CUST='EDWS_PROD.PROD_CMT_CAMPAIGN_01.SNP_'||'X09222'||'CT_POP';
select $CUST;
SELECT count(distinct sr_id) FROM identifier($CUST);


 --Create the customer pot
create or replace temp table CustSource_total as
    select sr_id,
           df_nn
    from identifier ($CUST)
    where sr_id not in (select distinct sr_id from customer_analytics.product_reporting.xcat_fallow_221019); ----remove the long term 8% fallow group from the selection

SET latest_week = (SELECT MAX(analysis_week)
                       FROM CUSTOMER_ANALYTICS.REPORTING.DIGITAL_NECTAR_ENGAGEMENT_SEGMENTATIONS);
select $latest_week;

--ALIGNING WITH TD EXCLUSION OF ANYONE 8 WEEK DN ACTIVE
CREATE OR REPLACE TEMP TABLE dig_nec_8weekactive_CUSTS as
SELECT distinct a.SR_ID,a.DF_NN, weeks_since_latest_Activity
FROM CustSource_total a
INNER JOIN EDWS_PROD.PROD_CMT_PRESENTATION.VW_CA_CUSTOMER_ACCOUNT b on a.sr_id=b.sr_id
INNER JOIN reporting.digital_nectar_engagement_segmentations c on sha2(cast(right(b.full_nectar_card_num, 11) as varchar(50)), 256) = c.HASHED_LOYALTY_ID
WHERE a.DF_NN = 1 AND weeks_since_latest_activity <= '8'
and c.ANALYSIS_WEEK = $latest_week;--Digital Nectar engaged

CREATE OR REPLACE TEMP TABLE custsource as
SELECT * FROM CustSource_total WHERE SR_ID not in (select sr_id from dig_nec_8weekactive_CUSTS);

select count(*), count(distinct sr_id) from custsource
-- 2125161,2125161


-- ***************************************************************************************
-- Olive and antipasti - low med high thres

select * from EDWS_PROD.PROD_CMT_PRESENTATION.VW_SKU limit 10



create or replace temp table Olive_antipasti as
select distinct  a.sku
                ,a.SKU_DESC
                ,sub_brand
                ,avg(case when b.avg_selling_price_12weeks>0 then b.avg_selling_price_12weeks else null end) as asp
                ,avg(case when b.margin_12weeks>0 then b.margin_12weeks else null end) as margin
    from EDWS_PROD.PROD_CMT_PRESENTATION.VW_SKU a
    inner join EDWS_PROD.PROD_CMT_PRESENTATION.VW_SKU_SUMMARY b
        on a.sku_key = b.sku_key
    where a.end_date is null
    and a.sub_category  in (691,817)
group by 1,2,3;

select count(distinct sku) from Olive_antipasti;


-- get transaction info
 create or replace temp table Olive_antipasti_trx as
    select stl.party_account_id as sr_id
            ,sum(stl.extended_price) as sales
            ,sum(stl.item_quantity) as quantity
         	,sum(item_quantity*margin)/sum(item_quantity)  as avg_margin

	from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_shopping_transaction_line as stl

	inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_ean as ean
		on stl.ean_key = ean.ean_key

	inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_c_sku_map as sku
		on ean.sku_key = sku.sku_key

	inner join Olive_antipasti as san on sku.sku = san.sku
    inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_c_location_map as lm
        on  lm.location_key = stl.location_key
        and lm.js_restaurant_ind = 'N' /* no restaurants */
--         where transaction_date between '2022-02-17' and '2022-04-22'
    	where transaction_date >= (current_date-180)
        and stl.extended_price>0 /*transaction value bigger than zero*/
		and stl.party_account_type_code in ('04') /*in store only for threshold*/
	group by 1;

select count(*),
       count(distinct sr_id) as custs,
       sum(quantity) as qty,
       sum(sales) as sales
from Olive_antipasti_trx;


--apply the criteria to ensure we select profitable customers
/* Get the average spend to decide what threshold each customer will be getting and ensure they have 0.33% avg margin */
create or replace temp table olive_antipasti_trx2 as
    select *,
           sales/quantity as avg_spd
    from Olive_antipasti_trx
    where
        quantity >=1
        and sales/quantity>= 2 --minimum TTD promo price £3 this easter
--       and avg_margin >= 0.33; --easter eggs have lower margin

select avg(avg_spd) from olive_antipasti_trx2 --2.612704433520
select * from olive_antipasti_trx2;

select percentile_cont(0.25) within group (order by avg_spd)  as avg_spend_p25
     , percentile_cont(0.5) within group (order by avg_spd)  as avg_spend_p50
     , percentile_cont(0.75) within group (order by avg_spd)  as avg_spend_p75
     , avg(avg_margin) as avg_margin
from olive_antipasti_trx2;

/*
 AVG_SPEND_P25,AVG_SPEND_P50,AVG_SPEND_P75,AVG_MARGIN
2.25    2.5     2.875
 */

create or replace temp table olive_antipasti_thresh as
select a.*,
			case when avg_spd BETWEEN 1 and 2.20 then 1
			 when avg_spd BETWEEN 2.21 and 2.50 then 2
			when avg_spd BETWEEN 2.51 and 2.8 then 3
			else 0 end as threshold
	from olive_antipasti_trx2 a;

--check counts
select
       threshold,
       count (*) as cts,
       count(distinct sr_id) as cts_cust_distinct
from  olive_antipasti_thresh
group by 1
order by 1;

/*
1-290792
2-361066
3-251714
*/