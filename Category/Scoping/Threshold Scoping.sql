use role RL_PROD_MARKETING_ANALYTICS;
use warehouse ADW_2XLARGE_ADHOC_WH;
use database CUSTOMER_ANALYTICS;
use schema PRODUCT_REPORTING;

create or replace temp table gifting as
    select cust.sr_id
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
         and a.transaction_date >= (current_date-180)
            and sku.end_date is null
            AND sub_category in (832)
   and sku.brand = 'OWN-LABEL'
      group by 1 ;


--apply the criteria to ensure we select profitable customers
/* Get the average spend to decide what threshold each customer will be getting and ensure they have 0.33% avg margin */
create or replace temp table gifting_trans as
    select *,
           SUM_EXTENDED_PRICE/NUM_TRANSACTIONS as avg_spd
    from gifting
    where 1=1
      -- and avg_margin > 0.33
      and SUM_ITEM_QUANTITY_26WEEKS >= 2
      and SUM_EXTENDED_PRICE/NUM_TRANSACTIONS>= 1.5;


select  percentile_cont(0.25) within group (order by AVG_SPD)  as avg_spend_p25
     , percentile_cont(0.5) within group (order by AVG_SPD)  as avg_spend_p50
     , percentile_cont(0.75) within group (order by AVG_SPD)  as avg_spend_p75
from gifting_trans;