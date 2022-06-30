USE ROLE RL_PROD_MARKETING_ANALYTICS;
USE WAREHOUSE WHS_PROD_MARKETING_ANALYTICS_LARGE;
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA TD_REPORTING;


-- how many Target / control
Select segment, count(distinct ec_ID) from "CUSTOMER_ANALYTICS"."TD_REPORTING"."TD02_2223_PETROL_DM" where target_control_flag = '0' group by segment;

-- TD08 Target: 267920 (Inactive:33487 Infrequent:183793 Lapsed:50640) Control: 14071 (Inactive:1731 Infrequent:9717 Lapsed:2623)
-- TD09 Target:  (Inactive:75966 Infrequent:300325 Lapsed:100349) Control: (Inactive:4072 Infrequent:15787 Lapsed:5269)
-- TD10 Target:  (Inactive:33537 Infrequent:184001 Lapsed:50842) Control: (Inactive:1763 Infrequent:9786 Lapsed: 2574)
-- TD11 Target:  (Inactive:33753 Infrequent:185240 Lapsed:51050) Control: (Inactive:1773 Infrequent:9807 Lapsed:2690)
-- TD01 Target:  (Inactive:46359 Infrequent:162090  Lapsed:60857) Control: (Inactive:5032 Infrequent:18183 Lapsed:6716)
-- TD02 Target:  (Inactive:31456 Infrequent: 183141 Lapsed:49524) Control: (Inactive:3474 Infrequent:20477 Lapsed:5385)


-- selection file
Select * from "CUSTOMER_ANALYTICS"."TD_REPORTING"."TD08_2122_PETROL_DM";

-- -- getting campaign info
-- create or replace table td_date_and_campaign_map_SH as
--                       select a.fin_year,
--                       a.fin_period,
--                       a.fin_week,
--                       a.quarter,
--                       a.period,
--                       b.cycle,
--                       a.week,
--                       a.week_number as td_week,
--                       a.week_start,
--                       a.week_end,
--                       CONCAT(cycle_year, '_TD', right(100+b.cycle,2)) as trade_driver,
--                       b.campaign_type,
--                       CONCAT(right(trade_driver, 4), '_', left(trade_driver,4), '_', b.campaign_type) as campaign,
--                       CONCAT(right(trade_driver, 4), '_', left(trade_driver,4), '_', b.snapshot_suffix) as campaign01_table_name,
--                       b.flowchartname,
--                       b.channel,
--                       b.redeeming,
--                       b.printing
--                from TD_DATE_MAP as a
--                         left join td_campaign_map as b
--                              on (a.fin_year = b.fin_year and a.week = b.week)
--                where a.week_start <= '2022-04-10'
--                  and a.week_end >= '2022-04-10'
--                  and campaign_type = 'Petrol_DM'
--                  and (b.redeeming = 1 or b.printing = 1);
--
--
-- --creating barcode lookup
-- create or replace temp table TD02_Barcodes as
-- Select
-- a.flowchartname,
-- a.campaignname,
-- a.flowchartid,
-- c.barcode,
-- OFFER_EFFECTIVE_DATE,
-- OFFER_EXPIRATION_DATE,
-- c.offer_cell,
-- substring(c.barcode,1,12) as coupon
-- from EDWS_PROD.PROD_CMT_PRESENTATION.vw_chrh_campaign_ec as a
-- inner join EDWS_PROD.PROD_CMT_PRESENTATION.vw_ch_fact_ec b
-- on a.flowchartid = b.flowchartid
-- inner join EDWS_PROD.PROD_CMT_PRESENTATION.vw_ch_offer_attribute_ec as c
-- on b.treatmentcode = c.treatmentcode
-- left join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_PARTY_ACCOUNT as ec
-- on ec.PARTY_ACCOUNT_ID = b.SR_ID
-- left join (select distinct trade_driver,
-- campaign_type,
-- campaign,
-- flowchartname from td_date_and_campaign_map_SH) as td
-- on a.flowchartname = td.flowchartname
-- and a.campaignname = td.trade_driver
-- where a.campaignname = '2223_TD02'
-- and a.flowchartname = 'Petrol_DM'
-- group by 1,2,3,4,5,6,7;
--
-- select * from EDWS_PROD.PROD_CMT_PRESENTATION.vw_ch_offer_attribute_ec;
--
--
-- -- adding dates
-- Create or replace table TD02 as
-- Select
-- *,
-- case when offer_cell like '%SPET%' then '0000004389064' else null end as Petrol_barcode,
-- case when offer_cell like '%SPET%' then '2022-05-28' else null end as petrol_cat_end_date,
-- '2022-04-10' as campaign_start_date,
-- '2022-05-15' as campaign_end_date,
-- '14' rolling_days_validity
-- from TD02_Barcodes;
--
-- create or replace temp table Petrol as
-- select distinct
--     pa.enterprise_customer_id as ec_id,
--     pl.party_account_id as sr_id,
--     pl.transaction_date,
--     to_time(right(cast(1000000 + pl.transaction_time as varchar(7)), 6)) as transaction_time,
--     pl.transaction_number,
--     st.party_account_type_code,
--     l.location,
--     st.transaction_value,
--     bl.petrol_barcode,
--     bl.barcode
--     bl.coupon,
--     bl.campaign_start_date,
-- bl.campaign_end_date,
-- bl.rolling_days_validity,
--     sum(pl.payment_value) as payment_value,
--     0 as print_qty,
--     1 as redeem_qty,
--     bl.campaignname,
--     '' as offer_code,
--     null as threshold,
--     0 as online_flag,
--     1 as pfs_red,
--     '0.06' as reward_value_penceoff
--
-- from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_payment_line pl
--
-- inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_shopping_transaction st
--     on st.location_key = pl.location_key
--         and st.transaction_date = pl.transaction_date
--         and st.transaction_time = pl.transaction_time
--         and st.transaction_number = pl.transaction_number
--         and st.party_account_type_code = pl.party_account_type_code
--         and st.party_account_id = pl.party_account_id
--         and st.till_number = pl.till_number
--
-- inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_c_location_map as l
--     on l.location_key = st.location_key
--
-- left join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_party_account pa
--     on pa.party_account_id = st.party_account_id
--
-- inner join (select distinct *
--             from TD02
--             where petrol_barcode is not null) as bl
--     on pl.coupon_id = bl.Petrol_barcode
--         and pl.transaction_date between to_date(bl.campaign_start_date) and to_date(bl.petrol_cat_end_date)
--
-- where pl.payment_type_code = '003'
-- group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,16,17,18,19,20,21,22,23;
--
-- -- add on week start and end
-- create or replace table TD02_Pet as
-- select a.*, b.fin_week, b.week_start, b.week_end
-- from petrol as a
-- left join TD_date_map as b
-- where a.transaction_date between b.week_start and b.week_end;
-- select * from tD02_pet;
--
--
-- create or replace table TD02_Red as
--     select a.campaignname,
--            a.EC_ID,
--            a.fin_week,
--            b.Segment,
--            b.Threshold,
--            a.transaction_date,
--            a.Location,
--            a.transaction_value,
--            a.petrol_barcode,
--            a.barcode,
--            a.coupon,
--            a.campaign_start_date,
--            a.campaign_end_date,
--            a.reward_value_penceoff,
--            a.week_start,
--            a.week_end
--     from TD02_Pet as a
-- Left join TD02_2223_Petrol_DM as b
-- on a.EC_ID = b.EC_ID
-- Group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16;
-- select * from TD02_Red;






-- -- no. of red by week
-- select fin_week, sum(redeem_qty)
-- from TD10_pet where EC_id in (select distinct ec_id from TD10_2122_PETROL_DM where Target_control_flag = 1)
-- group by 1 order by 1;
--
-- select * from TD10_2122_PETROL_DM limit 5;

-- -- test no dupes
-- select count(concat(ec_id, transaction_number)), count(distinct(concat(ec_id, transaction_number))) from petrol;
-- -- count:404,920
-- -- Count Distinct:20,246


create or replace table Petrol_PCA_redemptions as
    select * from TD08_red
Union select * from TD09_red
Union select * from TD10_red
Union Select * from TD11_red
Union select * from TD01_red
Union select * from TD02_red;


-- create or replace table Petrol_PCA_barcodes as
--     select * from TD08
-- Union select * from TD09
-- Union select * from TD10
-- Union Select * from TD11
-- Union select * from TD01
-- Union select * from TD02;
--
-- select * from Petrol_PCA_barcodes;

select * from Petrol_PCA_redemptions;

select campaignname,fin_week, sum(redeem_qty)
from Petrol_PCA_redemptions where EC_id in (select distinct ec_id from TD02_2223_PETROL_DM where Target_control_flag = 1)
and campaignname like '2223_TD02'
and petrol_barcode is not null
group by 1,2 order by 2;


select count(*) from Petrol_PCA_redemptions where EC_id in (select distinct ec_id from TD10_2122_PETROL_DM where Target_control_flag = 1)
and fin_week = 202144 and EC_id in (select distinct ec_id from TD09_2122_PETROL_DM where Target_control_flag = 1)


-- print tables
create or replace table Petrol_prints as
select distinct pf.party_account_id as sr_id,
                pf.transaction_date,
                pf.transaction_time,
                case when left(cast(sr_id as varchar(15)), 1) = '4' then '04'
                     when left(cast(sr_id as varchar(15)), 1) = '2' then '02' else '99' end as party_account_type_code,
                pf.location,
                bl.barcode,
                bl.petrol_barcode,
                pf.print_qty,
                0 as redeem_qty,
                bl.campaignname
from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_CATALINA_PRINT_FILE  as pf
         inner join Petrol_PCA_redemptions as bl
                    on substr(pf.barcode,1,7) = substr(bl.petrol_Barcode,7,7)
                    and pf.transaction_date between bl.campaign_start_date and bl.campaign_end_date
;


-- joining to ec_id
create or replace temp table Petrol_PCA_Redemptions_Prints2 as
    select  b.enterprise_customer_id as ec_id, a.*
from Petrol_PCA_Redemptions_Prints as a
left join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_PARTY_ACCOUNT as b
on a.sr_id = b.party_account_id
;

-- add redemptions and prints together
Create or replace table Petrol_PCA_Redemptions_Prints as
    select a.*, b.print_qty from Petrol_PCA_redemptions as a
left join Petrol_prints
on a.sr_id = b.Sr_id;

