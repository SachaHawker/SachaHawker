USE ROLE RL_PROD_MARKETING_ANALYTICS;
USE WAREHOUSE WHS_PROD_MARKETING_ANALYTICS_LARGE;
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA TD_REPORTING;





//TD04//
-- Make my own campaign map
create or replace table td_date_and_campaign_map_TD04_all as
                      select a.fin_year,
                      a.fin_period,
                      a.fin_week,
                      a.quarter,
                      a.period,
                      b.cycle,
                      a.week,
                      a.week_number as td_week,
                      a.week_start,
                      a.week_end,
                      CONCAT(cycle_year, '_TD', right(100+b.cycle,2)) as trade_driver,
                      b.campaign_type,
                      CONCAT(right(trade_driver, 4), '_', left(trade_driver,4), '_', b.campaign_type) as campaign,
                      CONCAT(right(trade_driver, 4), '_', left(trade_driver,4), '_', b.snapshot_suffix) as campaign01_table_name,
                      b.flowchartname,
                      b.channel,
                      b.redeeming,
                      b.printing
               from TD_DATE_MAP as a
                        left join td_campaign_map as b
                             on (a.fin_year = b.fin_year and a.week = b.week)
               where a.week_start <= '2022-07-17'
                 and a.week_end >= '2022-07-17';

select * from td_date_and_campaign_map_TD04_all;

select * from EDWS_PROD.PROD_CMT_PRESENTATION.vw_ch_offer_attribute_ec limit 5;

-- create barcode
create or replace table TP_TD04_barcode_all as
Select distinct
    c.REWARD_VALUE_POINTMULTI,
                c.reward_value_money,
                c.reward_value_fixed,
                c.reward_value_percentageoff,
                c.reward_value_petrol,
    td.campaign,
                c.barcode,
                c.offer_effective_date,
                c.offer_expiration_date
from EDWS_PROD.PROD_CMT_PRESENTATION.vw_chrh_campaign_ec as a
inner join EDWS_PROD.PROD_CMT_PRESENTATION.vw_ch_fact_ec b
on a.flowchartid = b.flowchartid
inner join EDWS_PROD.PROD_CMT_PRESENTATION.vw_ch_offer_attribute_ec as c
on b.treatmentcode = c.treatmentcode
left join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_PARTY_ACCOUNT as ec
on ec.PARTY_ACCOUNT_ID = b.SR_ID
left join td_date_and_campaign_map_TD04_all as td
on a.flowchartname = td.flowchartname
and a.campaignname = td.trade_driver
inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_shopping_transaction as d
on ec.PARTY_ACCOUNT_ID = d.party_account_ID
where a.campaignname = '2223_TD04'
and td.trade_driver = '2223_TD04';

select * from EDWS_PROD.PROD_CMT_PRESENTATION.vw_ch_offer_attribute_ec limit 5;
Select * from TP_TD04_barcode_all;


create or replace table vw_payment as
select party_account_id,
       party_account_type_code,
       transaction_date,
       transaction_time,
       transaction_number,
       payment_value,
       coupon_id,
       location_key,
       payment_type_code,
       till_number
from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_payment_line
where coupon_id <> ''
;

select * from vw_payment limit 10;


-- b) instore redemptions
-- pounds
create or replace table instore_pounds_TD04 as
select distinct pa.enterprise_customer_id as ec_id,
                pl.party_account_id as sr_id,
                pl.transaction_date,
                to_time(right(cast(1000000 + pl.transaction_time as varchar(7)), 6)) as transaction_time,
                pl.transaction_number,
                concat(pl.transaction_number,pl.transaction_time,pl.transaction_date) as transaction_identifier,
                st.party_account_type_code,
                l.location,
                st.transaction_value,
                bl.barcode,
                pl.payment_value,
                0 as print_qty,
                1 as redeem_qty,
                bl.campaign

from vw_payment as pl
         inner join (select * from TP_TD04_barcode_all) as bl
                    on (bl.barcode = pl.coupon_id
                        and pl.transaction_date between to_date(bl.offer_effective_date) and to_date(bl.offer_expiration_date))
         inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_shopping_transaction st
                    on st.location_key             = pl.location_key
                        and pl.transaction_date         = st.transaction_date
                        and st.transaction_time        = pl.transaction_time
                        and st.transaction_number      = pl.transaction_number
                        and st.party_account_type_code = pl.party_account_type_code
                        and st.party_account_id        = pl.party_account_id
                        and st.till_number             = pl.till_number
         inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_c_location_map as l
                    on l.location_key             = st.location_key
         left join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_party_account as pa
                   on pa.party_account_id        = st.party_account_id
where pl.payment_type_code       = '003';


select * from instore_pounds_TD04;

Create or replace table multiple_redeemptions as
select ec_id, transaction_identifier, campaign, count (*) as no_of_transactions
from instore_pounds_TD04
group by ec_id, transaction_identifier, campaign
having count(*) > 1;

select * from multiple_redeemptions;

Select count(*) from multiple_redeemptions;-- 429
select count (distinct ec_id) from multiple_redeemptions; -- 388


Select count (ec_id) from multiple_redeemptions where campaign = 'TD04_2223_Coupon_at_Till'; -- 361
Select count (distinct ec_id) from multiple_redeemptions where campaign = 'TD04_2223_Coupon_at_Till'; -- 345

Select count(distinct ec_id) from multiple_redeemptions where campaign = 'TD04_2223_Standard_DM'; -- 34
Select count(ec_id) from multiple_redeemptions where campaign = 'TD04_2223_Standard_DM'; -- 54

select count (distinct ec_id) from multiple_redeemptions where campaign = 'TD04_2223_Petrol_DM' -- 9
select count (ec_id) from multiple_redeemptions where campaign = 'TD04_2223_Petrol_DM' -- 10


select count (ec_id) from multiple_redeemptions where campaign = 'TD04_2223_Coupon_at_Till' and ec_id in (select ec_id from TD04_2223_Coupon_at_Till); -- only 231 are in selection file
select count (ec_id) from multiple_redeemptions where campaign = 'TD04_2223_Standard_DM' and ec_id in (select ec_id from TD04_2223_Standard_DM); -- 29 in the selection file
select count (ec_id) from multiple_redeemptions where campaign = 'TD04_2223_Petrol_DM' and ec_id in (select ec_id from TD04_2223_Petrol_DM); -- 9 in the selection file

-- Example of where there is two coupon redemptions:
-- select * from instore_pounds_TD04 where ec_id = '50000000178390';

// Selection file names: //
-- TD04_2223_Standard_DM
-- TD04_2223_Petrol_DMb
-- TD04_2223_Coupon_at_Till

select * from TP_TD04_barcode_all;

// POINTS //

create or replace table instore_points_TD04 as
select distinct pa.enterprise_customer_id as ec_id,
                point_red.party_account_id as sr_id,
                point_red.transaction_date,
                to_time(right(cast(1000000 + point_red.transaction_time as varchar(7)), 6)) as transaction_time,
                concat(point_red.transaction_number, point_red.transaction_time,point_red.transaction_date) as transaction_identifier,
                point_red.transaction_number,
                st.party_account_type_code,
                l.location,
                st.transaction_value,
                bl.barcode,
                point_red.points_earned*0.00425 as payment_value,
                0 as print_qty,
                1 as redeem_qty,
                bl.campaign,
                0 as online_flag,
                0 as pfs_red
from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_loyalty_coupon_redemption as point_red
         inner join (select * from TP_TD04_barcode_all) as bl
                    on substring(bl.barcode ,8 ,7) = point_red.coupon_id
                        and length(bl.barcode)=15
                        or (substring(bl.barcode, 6, 7) = point_red.coupon_id
                        and point_red.transaction_date between '2022-07-03' and '2022-08-06')

         inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_shopping_transaction as st
                    on st.location_key            = point_red.location_key
                        and point_red.transaction_date = case when st.ACTUAL_TRANSACTION_DATE_DELTA = 0 then st.transaction_date
                                                              when st.ACTUAL_TRANSACTION_DATE_DELTA = -1 then (st.transaction_date-1) end
                        and st.transaction_time        = point_red.transaction_time
                        and st.transaction_number      = point_red.transaction_number
                        and st.party_account_type_code = point_red.party_account_type_code
                        and st.party_account_id        = point_red.party_account_id
                        and st.till_number             = point_red.till_number

         inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_c_location_map as l
                    on l.location_key             = st.location_key
         left join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_party_account as pa
                   on pa.party_account_id        = st.party_account_id;


create or replace table TD04_points_redemptions as
select ec_id, transaction_identifier, count (*) as coupons_redeemed
from instore_points_TD04
group by ec_id, transaction_identifier


select ec_id, transaction_identifier,campaign from instore_points_TD04 where ec_id in (select ec_id from instore_points_TD04);

// Total transactions //

create or replace temp table campaign_crossover as
select distinct a.ec_id,
       a.transaction_identifier,
       a.campaign as point_campaign,
       b.campaign as pounds_campaign,
       count(distinct a.ec_id) as count_point,
       count(distinct b.ec_id) as count_pound
from instore_points_TD04 as a
FULL OUTER JOIN instore_pounds_TD04 as b
on a.ec_id = b.ec_id and a.transaction_identifier = b.transaction_identifier
group by 1,2,3,4;
-- 960,129

// count of customers that have redeemed in more than one campaign in the same transaction //
select sum(count_point) as count_points,
       sum(count_pound) as count_pounds,
       point_campaign,
       pounds_campaign
from campaign_crossover
group by 3,4;


select ec_id,
       count_point,
       count_pound,
       point_campaign,
       pounds_campaign
from campaign_crossover;





// IF CUSTOMERS ARE IN MORE THAN ONE CAMPAIN EACH PERIOD //
// confirming no crossover between DM and CAT
Create or replace temp table CAT_in_DM as
Select distinct ec_id
from TD04_2223_Coupon_at_Till
where ec_id in (select distinct ec_id
from TD04_2223_Standard_DM
where ec_id not in (select ec_id from "CUSTOMER_ANALYTICS"."DEVELOPMENT"."TD04_OPTOUT"));
-- 1,961 in both CAT & DM

select * from CAT_in_DM; -- zero - no cross over between CAT & DM

// Checking that optouts in DMs //
select ec_id
from TD04_2223_petrol_DM
where ec_id in (select ec_id from "CUSTOMER_ANALYTICS"."DEVELOPMENT"."TD04_OPTOUT")
-- 333 in petrol, 1197 in standard


// selecting CAT in DM //
Create or replace temp table  DM_minus_optout as
    select ec_id
from TD04_2223_Standard_DM
where ec_id not in (select ec_id from "CUSTOMER_ANALYTICS"."DEVELOPMENT"."TD04_OPTOUT") -- 270 in the optout

select * from DM_minus_optout;


// CROSS OVER //

Select distinct ec_id from TD04_2223_Coupon_at_Till where ec_id  in (select distinct ec_id from DM_minus_optout) -- 0


// FE, SDM, TP //
select count (*) from td04_2223_flash_email
where ec_id in (select ec_id from td04_2223_triple_points)
  and  ec_id in (select ec_id from  td04_2223_standard_DM);
--77107


// FE, SDM//
select count (*) from td04_2223_flash_email
where ec_id in (select ec_id from  td04_2223_standard_DM);
--153973

// TP, SDM//
select count (*) from td04_2223_triple_points
where ec_id in (select ec_id from  td04_2223_standard_DM);
--890709

// FE, CAT, TP //
select count (*) from td04_2223_flash_email
where ec_id in (select ec_id from td04_2223_triple_points)
  and  ec_id in (select ec_id from  td04_2223_coupon_at_till);
--288993

// FE, CAT //
select count (*) from td04_2223_flash_email
where ec_id in (select ec_id from  td04_2223_coupon_at_till);
--288993

// TP, CAT //
select count (*) from td04_2223_coupon_at_till
where ec_id in (select ec_id from  td04_2223_triple_points);
--1729673

// FE, TP //
select count (*) from td04_2223_flash_email
where ec_id in (select ec_id from td04_2223_triple_points);
-- 829844


// FE, PDM, TP //
select count (*) from td04_2223_flash_email
where ec_id in (select ec_id from td04_2223_triple_points)
  and  ec_id in (select ec_id from  td04_2223_Petrol_DM);
--61246

// PDM, TP //
select count (*) from td04_2223_Petrol_DM
where ec_id in (select ec_id from td04_2223_triple_points);
-- 215271

// PDM, FE //
select count (*) from td04_2223_Petrol_DM
where ec_id in (select ec_id from td04_2223_flash_email);
-- 89050



create or replace temp table campaign_selection as
    select ec_ID,
           case when ec_ID in (select ec_id from td04_2223_Petrol_DM where target_control_flag=1) then 1 else 0 end as In_Petrol_DM,
           case when ec_ID in (select ec_id from td04_2223_Standard_DM where target_control_flag=1) then 1 else 0 end as In_Standard_DM,
           case when ec_ID in (select ec_id from td04_2223_coupon_at_till where target_control_flag=1) then 1 else 0 end as In_CAT,
           case when ec_ID in (select ec_id from td04_2223_Triple_points where target_control_flag=1) then 1 else 0 end as In_Triple_Points,
           case when ec_ID in (select ec_id from td04_2223_Flash_email where target_control_flag=1) then 1 else 0 end as In_Flash
from td04_2223_Petrol_DM;

select count (distinct ec_ID)
from campaign_selection
where In_Petrol_DM =1




