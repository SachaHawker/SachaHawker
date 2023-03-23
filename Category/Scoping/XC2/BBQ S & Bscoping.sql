use role RL_PROD_MARKETING_ANALYTICS;
use warehouse ADW_2XLARGE_ADHOC_WH;
use database CUSTOMER_ANALYTICS;
use schema PRODUCT_REPORTING;

    select distinct a.sku_desc,
                a.SKU,
                a.sub_category,
                a.Sub_category_name,
                b.HFSS_ITEM_IND
from EDWS_PROD.PROD_CMT_PRESENTATION.vw_sku a
LEFT JOIN EDWS_PROD.PROD_CMT_PRESENTATION.VW_DIM_HFSS_ITEMS B ON A.SKU = B.ITEM_CD
where a. sub_category  in (420);


select distinct sub_category_name, sub_category
from EDWS_PROD.PROD_CMT_PRESENTATION.vw_sku a
LEFT JOIN EDWS_PROD.PROD_CMT_PRESENTATION.VW_DIM_HFSS_ITEMS B ON A.SKU = B.ITEM_CD
where a. sub_category  in (420);


/*
  Picnic/snack subcats:

  Sausage rolls  - 3149
  Pies and quiche - 3174
  summer range - 8851
  party bits - 3181
  buffet - 9940



  Party food -- 581
  Cold pies - 413
  Frozen pies and pastry - 945
  Antipasti&Olives - 691
  Dips - 817

  */



create or replace temp table sausage as
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
            and SKU.sub_category  in (420)

      group by 1,2 ;



--apply the criteria to ensure we select profitable customers
/* Get the average spend to decide what threshold each customer will be getting and ensure they have 0.33% avg margin */
create or replace temp table sausage_trans as
    select *,
           SUM_EXTENDED_PRICE/NUM_TRANSACTIONS as avg_spd
    from sausage
    where 1=1
      -- and avg_margin > 0.33
      and SUM_ITEM_QUANTITY_26WEEKS >= 2
      and SUM_EXTENDED_PRICE/NUM_TRANSACTIONS>= 1.5;


select  percentile_cont(0.25) within group (order by AVG_SPD)  as avg_spend_p25
     , percentile_cont(0.5) within group (order by AVG_SPD)  as avg_spend_p50
     , percentile_cont(0.75) within group (order by AVG_SPD)  as avg_spend_p75
from sausage_trans;

--

select
    a.sku_key
    ,a.sku
    ,c.avg_selling_price_12weeks as asp
    ,c.margin_12weeks as margin
    from EDWS_PROD.PROD_CMT_PRESENTATION.vw_sku as a
    inner join EDWS_PROD.PROD_CMT_PRESENTATION.vw_sku_summary as c
    on a.sku_key = c.sku_key
    where a.end_date is null
    and A.sub_category in(420)
    and asp <> 0
order by asp desc;

    select distinct a.sku_desc,
                a.SKU,
                a.sub_category,
                a.Sub_category_name,
                b.HFSS_ITEM_IND
from EDWS_PROD.PROD_CMT_PRESENTATION.vw_sku a
LEFT JOIN EDWS_PROD.PROD_CMT_PRESENTATION.VW_DIM_HFSS_ITEMS B ON A.SKU = B.ITEM_CD
where a. sub_category  in (420);





-- BURGER

use role RL_PROD_MARKETING_ANALYTICS;
use warehouse ADW_2XLARGE_ADHOC_WH;
use database CUSTOMER_ANALYTICS;
use schema PRODUCT_REPORTING;

    select distinct a.sku_desc,
                a.SKU,
                a.sub_category,
                a.Sub_category_name,
                b.HFSS_ITEM_IND
from EDWS_PROD.PROD_CMT_PRESENTATION.vw_sku a
LEFT JOIN EDWS_PROD.PROD_CMT_PRESENTATION.VW_DIM_HFSS_ITEMS B ON A.SKU = B.ITEM_CD
where a. sub_category  in (420);


select distinct sub_category_name, sub_category
from EDWS_PROD.PROD_CMT_PRESENTATION.vw_sku a
LEFT JOIN EDWS_PROD.PROD_CMT_PRESENTATION.VW_DIM_HFSS_ITEMS B ON A.SKU = B.ITEM_CD
where a. sub_category  in (420);


/*
  Picnic/snack subcats:

  Sausage rolls  - 3149
  Pies and quiche - 3174
  summer range - 8851
  party bits - 3181
  buffet - 9940



  Party food -- 581
  Cold pies - 413
  Frozen pies and pastry - 945
  Antipasti&Olives - 691
  Dips - 817

  */



create or replace temp table burger as
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
            and SKU.sub_category  in (419)

      group by 1,2 ;



--apply the criteria to ensure we select profitable customers
/* Get the average spend to decide what threshold each customer will be getting and ensure they have 0.33% avg margin */
create or replace temp table burger_trans as
    select *,
           SUM_EXTENDED_PRICE/NUM_TRANSACTIONS as avg_spd
    from burger
    where 1=1
      -- and avg_margin > 0.33
      and SUM_ITEM_QUANTITY_26WEEKS >= 2
      and SUM_EXTENDED_PRICE/NUM_TRANSACTIONS>= 1.5;


select  percentile_cont(0.25) within group (order by AVG_SPD)  as avg_spend_p25
     , percentile_cont(0.5) within group (order by AVG_SPD)  as avg_spend_p50
     , percentile_cont(0.75) within group (order by AVG_SPD)  as avg_spend_p75
from burger_trans;

--

select
    a.sku_key
    ,a.sku
    ,c.avg_selling_price_12weeks as asp
    ,c.margin_12weeks as margin
    from EDWS_PROD.PROD_CMT_PRESENTATION.vw_sku as a
    inner join EDWS_PROD.PROD_CMT_PRESENTATION.vw_sku_summary as c
    on a.sku_key = c.sku_key
    where a.end_date is null
    and A.sub_category in(419)
    and asp <> 0
order by asp asc;

    select distinct a.sku_desc,
                a.SKU,
                a.sub_category,
                a.Sub_category_name,
                b.HFSS_ITEM_IND
from EDWS_PROD.PROD_CMT_PRESENTATION.vw_sku a
LEFT JOIN EDWS_PROD.PROD_CMT_PRESENTATION.VW_DIM_HFSS_ITEMS B ON A.SKU = B.ITEM_CD
where a. sub_category  in (419);


















/*
  Picnic/snack subcats:

  Sausage rolls  - 3149
  Pies and quiche - 3174
  summer range - 8851
  party bits - 3181
  buffet - 9940



  Party food -- 581
  Cold pies - 413
  Frozen pies and pastry - 945
  Antipasti&Olives - 691
  Dips - 817

  */

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


create or replace table XC2_Scoping_BBQ as
select distinct a.sku_desc,
                a.SKU,
                a.sub_category,
                a.Sub_category_name,
                b.HFSS_ITEM_IND
from EDWS_PROD.PROD_CMT_PRESENTATION.vw_sku a
LEFT JOIN EDWS_PROD.PROD_CMT_PRESENTATION.VW_DIM_HFSS_ITEMS B ON A.SKU = B.ITEM_CD
where a. sub_category  in (475,419,616,420)
order by HFSS_ITEM_IND desc;


/*
BBQ:
Burgers - 419
Chicken portions - 616
Sausages - 420
Bread Rolls - 475

*/
