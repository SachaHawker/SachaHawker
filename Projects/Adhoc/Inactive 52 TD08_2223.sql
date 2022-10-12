-- Ran by Sacha Hawker on 21/09/2022 for TD08 DM
-- translated to sql by Erica Jackson 02/11/2021

/*Created by: Carol Brook on 28Jun2019
		- Updated by Beatriz Martin for Christmas Reactivation Campaign*/
/*Purpose of script: Select Nectar customers who are > 26 week inactive (grocery only) and decide what TD threshold to offer them*/


set yesterday = CURRENT_DATE() - 1;

set dt_52w_ago = $yesterday - 364; /* -364 days prior to yesterday */
set dt_26w_ago = $yesterday - 182; /* -182 days prior to yesterday */


/*Convenience stores*/

create or replace temp table convenience as
select distinct location_key
from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_location_trait_matrix
where location_trait_id=6141
;

/*active52w*/

create or replace temp table active52w_1 as
select stl.party_account_id
     ,stl.party_account_type_code
     ,lm.location_key
     , max(stl.transaction_date) as max_transaction_dt
     , count(*) as transactions_52w
from     EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_shopping_transaction as stl
             inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_c_location_map as lm
                        on    lm.location_key = stl.location_key
                            and lm.js_petrol_ind = 'N' /* no petrol stations */
                            and lm.js_restaurant_ind = 'N' /* no restaurants */
where stl.transaction_date between $dt_52w_ago and $yesterday
  and stl.party_account_type_code in ('04') /*instore transactions*/
  and stl.transaction_value>0 /*transaction value bigger than zero*/
group by 1,2,3
;

create or replace temp table  active52w_2 as
select
    party_account_id
     , party_account_type_code
     , max(max_transaction_dt) as max_transaction_dt
     , sum(transactions_52w) as transactions_52w
     , sum(case when location_key in (select distinct location_key from convenience) then transactions_52w else 0 end) as transactions_52w_convenience
from active52w_1
group by 1,2
;

create or replace temp table active52w_3 as
select *, iff(transactions_52w = transactions_52w_convenience, 1, 0) as is_convenience_only
from active52w_2
;

create or replace temp table active52w as
select * from active52w_3
;



/*active26w*/

create or replace temp table active26w as
select stl.party_account_id
     , count(*) as transactions_26w
from     EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_shopping_transaction as stl
             inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_c_location_map as lm
                        on    lm.location_key = stl.location_key
                            and lm.js_petrol_ind = 'N' /* no petrol stations */
                            and lm.js_restaurant_ind = 'N' /* no restaurants */
where stl.transaction_date between $dt_26w_ago and $yesterday
  and stl.party_account_type_code in ('04','02') /*instore and online transactions*/
  and stl.transaction_value>0 /*transaction value bigger than zero*/
group by 1
;


create or replace temp table base_table as
select
    a.party_account_id
     , a.party_account_type_code
     , a.max_transaction_dt
     , a.transactions_52w
     , a.transactions_52w_convenience
     , a.is_convenience_only
     , coalesce(b.transactions_26w,0) as transactions_26w
from active52w as a
         left join active26w as b
                   on a.party_account_id=b.party_account_id
;



/*26w inactive base summary (inactive in the last 26w BUT active in last 52w)*/

create or replace temp table inactive26w as
select *,
       max_transaction_dt - $yesterday as days_since_active,
       iff(transactions_26w = 0 and transactions_52w > 0, 'inactive26w, active52w', null) as status,
       dateadd(week, -12, max_transaction_dt) as max_transaction_dt_12WKprior

from base_table
;



/*All transactions in the 12 weeks prior to max_transaction_dt, incl Instore only (which will now be used in the threshold targeting)*/

create or replace temp table transactions_12wkprior_1 as
    select stl.party_account_id
               , stl.transaction_date
               , stl.transaction_time
               -- overall
               , count(*) as transactions_12WKprior
               , sum(stl.extended_price) as sum_transaction_value_12WKprior
               -- instore
               , sum(case when stl.party_account_type_code='04' then 1 else 0 end) as transactions_12WKpriorI
               , sum(case when stl.party_account_type_code='04' then stl.extended_price else 0 end) as sum_transaction_value_12WKpriorI

          from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_shopping_transaction_line as stl
                   inner join inactive26w as inactive
                              on stl.party_account_id=inactive.party_account_id
                   inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_ean as e
                              on    e.ean_key = stl.ean_key
                   inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_c_sku_map as s
                              on    e.sku_key = s.sku_key
                   inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_posass_flat2 as pos
                              on    pos.sub_category_key = s.sub_category_key
                   inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_sub_category_map as sub
                              on s.sub_category_key = sub.sub_category_key
                   inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_c_location_map as lm
                              on    lm.location_key = stl.location_key
                                  and lm.js_petrol_ind = 'N' -- no petrol stations
                                  and lm.js_restaurant_ind = 'N' -- no restaurants

          where stl.transaction_date between inactive.max_transaction_dt_12WKprior and inactive.max_transaction_dt
            and stl.party_account_type_code in ('04','02') /*instore and online transactions*/
            and stl.extended_price>0 /*transaction value bigger than zero*/
            and s.sku_key not in (select distinct sku_key from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_nectar_exclusions)
        group by 1,2,3
;

create or replace temp table transactions_12WKprior as
select party_account_id

     , count(*) as transactions_12WKprior
     , sum(transactions_12WKprior) as sum_transaction_value_12WKprior
     , avg(sum_transaction_value_12WKprior) as avg_transaction_value_12WKprior

     , sum(transactions_12WKpriorI) as transactions_12WKpriorI
     , sum(sum_transaction_value_12WKpriorI) as sum_transaction_value_12WKpriorI
     , avg(sum_transaction_value_12WKpriorI) as avg_transaction_value_12WKpriorI

from transactions_12wkprior_1
group by 1
;

/*Base summary V2*/
create or replace temp table inactive26w as
select
    a.party_account_id
     , a.party_account_type_code
     , a.max_transaction_dt
     , a.transactions_52w
     , a.transactions_52w_convenience
     , a.is_convenience_only
     , a.transactions_26w
     , a.status
     , a.days_since_active
     , b.transactions_12WKprior
     , b.sum_transaction_value_12WKprior
     , b.avg_transaction_value_12WKprior
     , b.transactions_12WKpriorI
     , b.sum_transaction_value_12WKpriorI
     , b.avg_transaction_value_12WKpriorI
from inactive26w as a
         left join transactions_12WKprior as b
                   on a.party_account_id=b.party_account_id;
;


/*Base summary V3 (final) - Pull out EC_ID*/


create or replace temp table inactive26w_with_ecid as
select a.*
     , b.enterprise_customer_id
     , b.household_id
     , b.sty_reg_loyalty_account_no
     , c.week_no /*MAX transaction date*/
     , case when a.party_account_type_code='04' then 1
            when a.party_account_type_code='02' and b.sty_reg_loyalty_account_no<>'' then 1
            else 0 end as is_nectar
from inactive26w as a
         left join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_party_account as b
                   on a.party_account_id=b.party_account_id
         left join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_date_map as c
                   on a.max_transaction_dt=c.calendar_date
;


create or replace temp table ECID_agg as
select
    enterprise_customer_id
     , sum(transactions_26w) as sum_transactions_26w
from inactive26w_with_ecid
group by 1;
;


create or replace temp table inactive26w_with_ecid as
select a.*
     , case when b.sum_transactions_26w=0 then 1 else 0 end as is_26w_inactive_ECID
from inactive26w_with_ecid as a
         left join ECID_agg as b
                   on a.enterprise_customer_id=b.enterprise_customer_id;
;

create or replace temp table inactive26w_with_ecid as
select *
from inactive26w_with_ecid
where is_26w_inactive_ECID = 1
; /*ec_id 26w inactive*/


-- contactability
create or replace temp table inactive26w_with_ecid_and_contactability as
select a.*, b.jsgroc_dm
from inactive26w_with_ecid as a

         left join EDWS_PROD.PROD_CMT_PRESENTATION.VW_sr_RT_SUPRESSION as b
                   on a.party_account_id = b.SR_ID
;

-- save to CAMPAIGN_01
create or replace table EDWS_PROD.PROD_CMT_CAMPAIGN_01.TD_TD08_2223_26WK_INACTIVE_CUSTOMERS as
select party_account_id, enterprise_customer_id, household_id, week_no as last_week_active, jsgroc_dm,
       avg_transaction_value_12wkpriori as avg_transaction_value_12wk_prior_instore,

       -- instore thresh. if the customer hasn't got an average online spend then use their total (online) spend
       case when (avg_transaction_value_12wkpriori is null or avg_transaction_value_12wkpriori = 0) then
                case when avg_transaction_value_12wkprior is null then 30
                     when round(avg_transaction_value_12wkprior) < 30 then 30
                     when round(avg_transaction_value_12wkprior) > 80 then 80
                     else round((round(avg_transaction_value_12wkprior)/10))*10
                    end
            when round(avg_transaction_value_12wkpriori) < 30 then 30
            when round(avg_transaction_value_12wkpriori) > 80 then 80
            else round((round(avg_transaction_value_12wkpriori)/10))*10
           end as instore_thresh
from inactive26w_with_ecid_and_contactability
;


select last_week_active, count(distinct enterprise_customer_id)
from EDWS_PROD.PROD_CMT_CAMPAIGN_01.TD_TD08_2223_26WK_INACTIVE_CUSTOMERS
where jsgroc_dm in ('Y', 'LI', 'LIN')
group by 1
;


select * from EDWS_PROD.PROD_CMT_CAMPAIGN_01.TD_TD08_2223_26WK_INACTIVE_CUSTOMERS
where jsgroc_dm in ('Y', 'LI', 'LIN');

// Volumes //
-- Total in file: 681,086
-- Total minus 10% control: 612,977

select count (distinct a.enterprise_customer_id)
from EDWS_PROD.PROD_CMT_CAMPAIGN_01.TD_TD08_2223_26WK_INACTIVE_CUSTOMERS a
inner join  "EDWS_PROD"."PROD_CMT_PRESENTATION"."VW_CA_CUSTOMER_ACCOUNT" b
on a.enterprise_customer_id = b.ec_id
where jsgroc_dm in ('Y', 'LI', 'LIN')
and LMS_status_code in ('AC', 'A');






select * from EDWS_PROD.PROD_CMT_CAMPAIGN_01.TD_TD08_2223_26WK_INACTIVE_CUSTOMERS where enterprise_customer_id = '50000000331176';
select * from "EDWS_PROD"."PROD_CMT_PRESENTATION"."VW_CA_CUSTOMER_ACCOUNT";