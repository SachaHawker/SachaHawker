USE ROLE RL_PROD_MARKETING_ANALYTICS;
USE WAREHOUSE WHS_PROD_MARKETING_ANALYTICS_LARGE;
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA TD_REPORTING;



-- Create my own date and campaign map, normal one is live, this is just for this campaign
create or replace table td_date_and_campaign_map_SH as
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
               where a.week_start <= '2022-02-01'
                 and a.week_end >= '2022-02-01'
                 and campaign_type = 'Petrol_DM'
                 and (b.redeeming = 1 or b.printing = 1);


select * from EDWS_PROD.PROD_CMT_PRESENTATION.vw_ch_offer_attribute_ec where offer_cell like 'BL%' limit 5;

--creating barcode lookup, all DM barcodes for money off and fuel weeks for each threshold
create or replace table TD10_Barcodes1 as
Select
a.flowchartname,
a.campaignname,
a.flowchartid,
c.barcode,
OFFER_EFFECTIVE_DATE,
OFFER_EXPIRATION_DATE,
c.offer_cell,
substring(c.barcode,1,12) as coupon
from EDWS_PROD.PROD_CMT_PRESENTATION.vw_chrh_campaign_ec as a
inner join EDWS_PROD.PROD_CMT_PRESENTATION.vw_ch_fact_ec b
on a.flowchartid = b.flowchartid
inner join EDWS_PROD.PROD_CMT_PRESENTATION.vw_ch_offer_attribute_ec as c
on b.treatmentcode = c.treatmentcode
left join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_PARTY_ACCOUNT as ec
on ec.PARTY_ACCOUNT_ID = b.SR_ID
left join (select distinct trade_driver,
campaign_type,
campaign,
flowchartname from td_date_and_campaign_map_SH) as td
on a.flowchartname = td.flowchartname
and a.campaignname = td.trade_driver
where a.campaignname = '2122_TD10'
and a.flowchartname = 'Petrol_DM'
group by 1,2,3,4,5,6,7;

select * from TD10_Barcodes1 where offer_cell = 'BL_9263_9877';

select * from TD10_Barcodes2;

-- add petrol barcode (6p) and start, end and rolling date
Create or replace table TD10_Barcodes2 as
Select
*,
case when offer_cell like '%SPET%' then '0000004389064' else null end as Petrol_barcode,
case when offer_cell like '%SPET%' then '2022-02-12' else null end as petrol_cat_end_date,
'2022-01-02' as campaign_start_date,
'2022-01-30' as campaign_end_date,
'14' rolling_days_validity
from TD10_Barcodes1;


-- Create redemptions for fuel weeks
create or replace table TD10_CAT_Redemption1 as
select distinct
    pa.enterprise_customer_id as ec_id,
    pl.party_account_id as sr_id,
    pl.transaction_date,
    to_time(right(cast(1000000 + pl.transaction_time as varchar(7)), 6)) as transaction_time,
    pl.transaction_number,
    st.party_account_type_code,
    l.location,
    st.transaction_value,
    bl.petrol_barcode,
    bl.campaign_start_date,
    0 as print_qty,
    1 as redeem_qty,
    bl.campaignname,
    '' as offer_code,
    null as threshold,
    0 as online_flag,
    1 as pfs_red,
    '0.06' as reward_value_penceoff,
    sum(pl.payment_value) as payment_value

from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_payment_line pl

inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_shopping_transaction st
    on st.location_key = pl.location_key
        and st.transaction_date = pl.transaction_date
        and st.transaction_time = pl.transaction_time
        and st.transaction_number = pl.transaction_number
        and st.party_account_type_code = pl.party_account_type_code
        and st.party_account_id = pl.party_account_id
        and st.till_number = pl.till_number

inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_c_location_map as l
    on l.location_key = st.location_key

left join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_party_account pa
    on pa.party_account_id = st.party_account_id

inner join (select distinct         campaignname,
                                    petrol_barcode,
                                    campaign_start_date,
                                    petrol_cat_end_date
            from TD10_Barcodes2
            where petrol_barcode is not null) as bl
    on pl.coupon_id = bl.Petrol_barcode
        and pl.transaction_date between to_date(bl.campaign_start_date) and to_date(bl.petrol_cat_end_date)

where pl.payment_type_code = '003'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,16,17,18;

select * from TD10_CAT_Redemption1 where ec_id = '50000011716472';

-- add on week start and end to fuel week redemptions
create or replace table TD10_CAT_Redemption2 as
select a.*, b.fin_week, b.week_start, b.week_end
from TD10_CAT_Redemption1 as a
left join TD_date_map as b
where a.transaction_date between b.week_start and b.week_end;
select * from TD10_Redemption2 where ec_id = '50000011716472';


select count (*) from TD10_Redemption2;
-- 15,169 - This is all redeemers, including mal redeemers


-- Redeemers just in that campaign
create or replace table TD10_CAT_Redemption3 as
select * from TD10_Redemption2
where ec_id in (select ec_id from TD10_2122_Petrol_DM where target_control_flag = 1);
Select count(*) from TD10_CAT_Redemption3;
-- 9,044 - Redeemers in total (may have some from other campaigns- reuse barcodes)

select * from TD10_CAT_Redemption3 where ec_id = '50000011716472'


-- Create prints of the fuel week
create or replace table TD10_Prints1 as
select distinct pf.party_account_id as sr_id,
                pf.transaction_date,
                pf.transaction_time,
                case when left(cast(sr_id as varchar(15)), 1) = '4' then '04'
                     when left(cast(sr_id as varchar(15)), 1) = '2' then '02' else '99' end as party_account_type_code,
                pf.location,
                bl.petrol_barcode,
                pf.print_qty,
                0 as redeem_qty,
                bl.campaignname
from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_CATALINA_PRINT_FILE  as pf
         inner join TD10_Barcodes2 as bl
                    on substr(pf.barcode,1,7) = substr(bl.petrol_Barcode,7,7)
                    and pf.transaction_date between bl.campaign_start_date and bl.campaign_end_date;]

select * from td11_2122_petrol_dm where ec_id = '50000011716472';




-- add on EC_ID
create or replace table TD10_Prints2 as
    select  b.enterprise_customer_id as ec_id, a.*
from TD10_Prints1 as a
left join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_PARTY_ACCOUNT as b
on a.sr_id = b.party_account_id;

select * from TD10_Prints2 where ec_id = '50000011716472'
-- total prints 10,229 - this includes mal redeemers



-- only prints from people that are in selection file
Create or replace table TD10_Prints3 as
    Select * from TD10_Prints2
where EC_ID in (select ec_id from TD10_2122_Petrol_DM where target_control_flag = 1);
Select * from TD10_Prints3;
--7,053 - prints from only those in selection file

create or replace table TD10_Prints4 as
    select a.*,
           b.fin_week,
           b.week_start,
           b.week_end
    from TD10_Prints3 as a
left join td_date_map as b
where a.transaction_date between b.week_start and b.week_end;

select * from td10_prints4 where ec_id = '50000020856112';
--2022-01-29 --13:36:31
--2022-01-29 -- 13:41:11
--2022-01-15 --10:45:10
--2022-01-15 -- 10:46:42

select * from TD10_Prints4 where ec_id = '50000011716472';



create or replace table TD10_Redemption2 as
select a.*, b.fin_week, b.week_start, b.week_end
from TD10_Redemption1 as a
left join TD_date_map as b
where a.transaction_date between b.week_start and b.week_end;
select * from TD10_Redemption2;



-- Redeemers where redemption is after print date, excluding those from other campaigns
create or replace table TD10_Redemption4 as
select distinct b.ec_id,
       b.redeem_qty,
       b.red_date,
       case when b.red_date < a.print_date then 1 else 0 end as unrelated_redemption_flag,
       c.threshold,
       c.segment,
       b.Payment_value,
       b.fin_week
from (select ec_id,
             sum(print_qty) as print_qty,
             min(transaction_date) as print_date,
             fin_week
from TD10_Prints4 group by 1,4) as a
left join (select ec_id,
                  redeem_qty as redeem_qty,
                  transaction_date as red_date,
                  payment_value as Payment_value,
                  fin_week
from TD10_CAT_Redemption3) as b on a.ec_id = b.ec_id
Left join TD10_2122_Petrol_DM as c
on b.ec_id = c.ec_id
where unrelated_redemption_flag = 0 and redeem_qty is not null;

select * from TD10_Redemption4 where fin_week is null;

select * from TD10_Redemption4; -- customer level

-- Petrol redemptions by week
select fin_week, count (*) from TD10_Redemption4 group by 1;
-- 5496

-- total volumes by redemptions
select  count (*), count (distinct ec_id) from TD10_Redemption4;
select count (distinct ec_Id) as volume, redemptions
from
       (select ec_id, sum(redeem_qty) as redemptions
           from TD10_Redemption4 group by 1)
group by 2;


-- customers that have redeemed more than once - use this to see if they are in the next campaigns selection file
select *
from
       (select ec_id, sum(redeem_qty) as redemptions
           from TD10_Redemption4 group by 1)
where redemptions > 2;





-- Select a.ec_id, number_of_prints, Number_of_petrol_redemptions
-- From
--      (Select ec_id, Count(*) number_of_prints
--       From TD10_Prints4
--     Group by ec_id
--      ) a
-- Left join
--      (select ec_id, Count(*) Number_of_petrol_redemptions
--       from TD10_Redemption3
--       group by ec_id
--      ) b on a.ec_id = b.ec_id
-- group by 1,2,3



-- MONEY OFF WEEKS
create or replace temp table MO_Payment as
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


-- redemptions for fuel and money off weeks
create or replace temp table TD10_DM_Redemptions1 as
select          pa.enterprise_customer_id as ec_id,
                pl.party_account_id as sr_id,
                pl.transaction_date,
                to_time(right(cast(1000000 + pl.transaction_time as varchar(7)), 6)) as transaction_time,
                pl.transaction_number,
                st.party_account_type_code,
                l.location,
                st.transaction_value,
                bl.barcode,
                pl.payment_value,
                0 as print_qty,
                1 as redeem_qty,
                bl.campaignname,
                bl.offer_cell,
                bl.coupon,
                bl.campaign_start_date,
                bl.campaign_end_date
from MO_Payment as pl
         inner join (select * from TD10_Barcodes2) as bl
                    on (bl.barcode = pl.coupon_id
                        and pl.transaction_date between to_date(bl.campaign_start_date) and to_date(bl.campaign_end_date))
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
where pl.payment_type_code  = '003'
;




-- Redemptions from those that were targeted in selection file - petrol and Money Off weeks
create or replace table TD10_DM_Redemptions2 as
select * from TD10_DM_Redemptions1
where ec_id in (select ec_id from TD10_2122_Petrol_DM where target_control_flag = 1);
Select count(*) from TD10_DM_Redemptions2;
Select * from TD10_DM_Redemptions2
-- 29,176 - total redeeemers in this campaign (petrol and money off)

-- add fin week
create or replace table TD10_DM_Redemptions3 as
select a.*, b.fin_week, b.week_start, b.week_end
from TD10_Redemptions2 as a
left join TD_date_map as b
where a.transaction_date between b.week_start and b.week_end;
select * from TD10_Redemptions3;


-- Redemptions for just fuel weeks in the selection file
create or replace table TD10_Fuel_DM_Redemptions as
select a.ec_id,
       a.sr_id,
       a.redeem_qty,
       min(a.transaction_date) as red_date,
       a.transaction_value,
       a.fin_week,
       b.Threshold,
       b.Segment,
       a.offer_cell,
       a.payment_value as Payment_value,
       a.transaction_time
From TD10_Redemptions3 as a
Left join TD10_2122_Petrol_DM as b
on a.ec_id = b.ec_id
where offer_cell  like 'SPET%'
Group by 1,2,3,5,6,7,8,9,10,11;

-- redemptions just for MO weeks
create or replace table TD10_MO_DM_Redemptions as
select a.ec_id,
       a.redeem_qty,
       min(a.transaction_date) as red_date,
       a.transaction_value,
       a.fin_week,
       b.Threshold,
       b.Segment,
       a.offer_cell,
       a.payment_value as Payment_value,
       a.transaction_time
From TD10_DM_Redemptions3 as a
Left join TD10_2122_Petrol_DM as b
on a.ec_id = b.ec_id
where offer_cell not like 'SPET%'
Group by 1,2,4,5,6,7,8,9,10;
Select * from TD10_MO_DM_Redemptions
-- 22284


-- DM REDEMPTIONS
--2022-01-29 -- 13:07:00
--2022-01-15-- 05:01:43

-- PRINTS
--2022-01-29 --13:36:31
--2022-01-29 -- 13:41:11
--2022-01-15 --10:45:10
--2022-01-15 -- 10:46:42




-- Fuel DM redemptions
-- transactional level
create or replace table TD10_Fuel_DM_Redemptions1 as
select  b.ec_id,
       b.redeem_qty,
       b.red_date,
       c.threshold,
       c.segment,
       b.transaction_value,
       b.fin_week,
b. sr_id
from TD10_Fuel_DM_Redemptions as b
Left join TD10_2122_Petrol_DM as c
on b.ec_id = c.ec_id
select * from TD10_Fuel_DM_Redemptions1;
-- 6,892

select distinct ec_id, sum(redeem_qty) as vol from TD10_Fuel_DM_Redemptions1 group by 1 having vol > 1 ;
-- 895 with more than 1 Dm redemption

-- customer level
create or replace table TD10_Fuel_DM_Redemptions2 as
    select distinct ec_id,
           threshold,
           segment,
           sum(redeem_qty) as redeem_qty,
        sr_id
from TD10_Fuel_DM_Redemptions1
group by 1,2,3,5;

select * from TD10_Fuel_DM_Redemptions2;


select count (distinct ec_id), redeem_qty from TD10_Fuel_DM_Redemptions2 where redeem_qty >1 group by 2;
-- 892 with 2 redemptions




-- Prints
-- transaction level
create or replace temp table td10_prints5 as
    select distinct
    a.*,
           b.segment,
           b.threshold
        from TD10_Prints4  as a
Left join TD10_2122_Petrol_DM as b
on a.ec_id = b.ec_id;
-- 7,053

-- PRINTS
--customer level
create or replace temp table TD10_Prints6 as
    select ec_id,
           threshold,
           segment,
           sum(print_qty) as print_qty
from td10_prints5
group by 1,2,3;

Select * from td10_prints6
-- 6,059




-- Petrol CAT redemptions
create or replace table TD10_Redemption5 as
select distinct b.ec_id,
       b.redeem_qty,
       b.red_date,
       case when b.red_date < a.print_date then 1 else 0 end as unrelated_redemption_flag,
       c.threshold,
       c.segment,
       b.Payment_value,
       b.fin_week
from (select ec_id,
             sum(print_qty) as print_qty,
             min(transaction_date) as print_date,
             fin_week
from TD10_Prints4 group by 1,4) as a
left join (select ec_id,
                  redeem_qty as redeem_qty,
                  transaction_date as red_date,
                  payment_value as Payment_value,
                  fin_week
from TD10_CAT_Redemption3) as b on a.ec_id = b.ec_id
Left join TD10_2122_Petrol_DM as c
on b.ec_id = c.ec_id
where unrelated_redemption_flag = 0 and redeem_qty is not null;
select * from TD_REPORTING.TD10_Redemption4
-- 5,496


-- dm fuel redemptions - 6,890
-- prints - 6,962
-- CAT redemptions - 5,496


create or replace table Redemption_print_summary as
select
    coalesce(a.ec_id, b.ec_id) as ec_id,
       a.print_qty as prints,
       b.redeem_qty as DM_redemptions,
       c.redeem_qty as CAT_redemptions,
    coalesce(a.threshold, b.threshold, c.threshold) as threshold,
       coalesce(a.segment, b.segment, c.threshold) as segment
from TD10_Prints6 as a
full outer join TD10_Fuel_DM_Redemptions2 as b
on a.ec_id = b.ec_id
full outer join td10_CAT_redemption3 as c
on a.ec_id = c.ec_id



select CAT_redemptions, count (ec_id) from TD_REPORTING.Redemption_print_summary group by 1;
-- 7090


-- find how many is reedeming once or more than once
select * from TD10_MO_Redemptions4;
Select fin_week, redeem_qty, count (distinct ec_id), count (*) from TD10_MO_Redemptions4 group by 1,2;





create or replace table summary_petrol as
Select distinct a.ec_id,a.sr_id, a.number_of_prints, b.Number_of_CAT_redemptions, c.Number_of_DM_redemptions
From
     (Select ec_id, sr_id, Count(*) number_of_prints
      From TD10_Prints4
    Group by 1,2
     ) a
Left join
     (select ec_id, sr_id, Count(*) Number_of_CAT_redemptions
      from TD10_CAT_Redemption3
      group by 1,2
     ) b on a.ec_id = b.ec_id and a.sr_id=b.sr_id
left join
         (select ec_id, sr_id, count (*),
             sum(redeem_qty) as Number_of_DM_redemptions
             from TD10_Fuel_DM_Redemptions2
             group by 1,2
             ) c on a.ec_id = c.ec_id and a.sr_id=c.sr_id;

select count(substr(sr_id,3))as volume, number_of_prints, Number_of_DM_redemptions from summary_petrol where number_of_prints > number_of_DM_Redemptions group by 2,3;
select substr(sr_id,3) as volume, number_of_prints, Number_of_DM_redemptions from summary_petrol where number_of_prints > number_of_DM_Redemptions group by 1,2,3;
select substr(sr_id,3) as volume, number_of_prints, Number_of_DM_redemptions from summary_petrol where number_of_DM_Redemptions is null group by 1,2,3;
select substr(sr_id,3) as volume, number_of_prints, Number_of_DM_redemptions from summary_petrol where number_of_DM_redemptions is null group by 1,2,3;

-- final summary of prints and DM red
select count(ec_id) as volume, number_of_prints, Number_of_DM_redemptions from summary_petrol group by 2,3;

Select * from petrol_backup_prints;



