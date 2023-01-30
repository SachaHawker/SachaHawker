use role RL_PROD_MARKETING_ANALYTICS;
use warehouse ADW_2XLARGE_ADHOC_WH;
use database CUSTOMER_ANALYTICS;
use schema PRODUCT_REPORTING;

create or replace temp table FLOURISH as
    select cust.sr_id
         ,SKU.SKU_desc
         ,SUM(unit_Price) as unit_price
         ,sum(EXTENDED_PRICE) as SUM_EXTENDED_PRICE
         ,max(transaction_date)  as LAST_PURCHASE_DATE
         ,count(distinct transaction_date) as NUM_TRANSACTIONS
         ,sum(item_quantity) as SUM_ITEM_QUANTITY_26WEEKS
--       ,sum(item_quantity*margin)/sum(item_quantity)  as avg_margin
         ,sum(case when transaction_date >= (current_date-84) then item_quantity else 0 end) as SUM_ITEM_QUANTITY_12WEEKS

      from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_SHOPPING_TRANSACTION_LINE as a
                  inner join CustSource cust on cust.sr_id=a.party_account_id
                  inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_EAN d on a.ean_key=d.ean_key
                  inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_SKU_MAP as e on e.sku_key = d.sku_key
                        inner join EDWS_PROD.PROD_CMT_PRESENTATION.vw_sku as sku on sku.sku_key = e.sku_key
                      inner join EDWS_PROD.PROD_CMT_PRESENTATION.VW_CA_CUSTOMER_ACCOUNT b on cust.sR_id = b.sr_id

      where a.item_refunded_ind = 'N'
         and a.extended_price > 0
--          and a.transaction_date >= (current_date-180)
            and sku.end_date is null
            AND SKU.SKU in (8128931,
8128924,
8128929,
8137097,
8137099,
8077678,
8077928,
8129387,
8129380,
8129382,
8129373,
8129376,
8133373,
8137583,
8137557,
8137555,
8044092,
8133377,
8133381,
8133379,
8137553,
8044060,
8133375,
8044056,
8043648,
8043757,
8129891,
8129893,
8014640,
8107553,
8135845,
8135847,
8030654,
8030662,
8129535,
8129554,
8129552,
7642799,
670111,
425803,
1185751,
8134687,
8134685,
8131718,
8131720,
8133354,
8133357,
8133352,
8135849,
8131202,
8131204,
8131198,
8138100,
8136915,
8131196,
8131186,
8136917,
8130935,
8130937,
8130939,
8134207,
8134204,
8134199,
8134201
)

      group by 1,2 ;

select count(*) from FLOURISH; --218079


--apply the criteria to ensure we select profitable customers
/* Get the average spend to decide what threshold each customer will be getting and ensure they have 0.33% avg margin */
create or replace temp table FLOURISH_trans as
    select *,
           SUM_EXTENDED_PRICE/NUM_TRANSACTIONS as avg_spd
    from FLOURISH
    where 1=1
      -- and avg_margin > 0.33
      and SUM_ITEM_QUANTITY_26WEEKS >= 2
      and SUM_EXTENDED_PRICE/NUM_TRANSACTIONS>= 1.5;


select  percentile_cont(0.25) within group (order by AVG_SPD)  as avg_spend_p25
     , percentile_cont(0.5) within group (order by AVG_SPD)  as avg_spend_p50
     , percentile_cont(0.75) within group (order by AVG_SPD)  as avg_spend_p75
from FLOURISH_trans;









select * from EDWS_PROD.PROD_CMT_PRESENTATION.vw_sku where sku_desc = 'JS Multifruit 0% Yoghurt Drink 8x100g';
select * from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_SKU_MAP limit 5;
select * from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_EAN limit 5;
select * from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_SHOPPING_TRANSACTION_LINE limit 5;





/*select sku.SKU_desc, sku.sku, sku.end_date from
             EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_EAN d
                  inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_SKU_MAP as e on e.sku_key = d.sku_key
                        inner join EDWS_PROD.PROD_CMT_PRESENTATION.vw_sku as sku on sku.sku_key = e.sku_key
where sku.sku in (8128931,
8128924,
8128929,
8137097,
8137099,
8077678,
8077928,
8129387,
8129380,
8129382,
8129373,
8129376,
8133373,
8137583,
8137557,
8137555,
8044092,
8133377,
8133381,
8133379,
8137553,
8044060,
8133375,
8044056,
8043648,
8043757,
8129891,
8129893,
8014640,
8107553,
8135845,
8135847,
8030654,
8030662,
8129535,
8129554,
8129552,
7642799,
670111,
425803,
1185751,
8134687,
8134685,
8131718,
8131720,
8133354,
8133357,
8133352,
8135849,
8131202,
8131204,
8131198,
8138100,
8136915,
8131196,
8131186,
8136917,
8130935,
8130937,
8130939,
8134207,
8134204,
8134199,
8134201
)
;*/


