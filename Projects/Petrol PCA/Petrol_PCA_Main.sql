USE ROLE RL_PROD_MARKETING_ANALYTICS;
USE WAREHOUSE WHS_PROD_MARKETING_ANALYTICS_LARGE;
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA TD_REPORTING;



-- --TD08
Set campaignname = '2122_TD08';
SET campaign_start_date = '2021-10-31';
SET campaign_end_date = '2021-12-04';
SET rolling_date = '2021-12-18';
SET td ='TD08';
SET year ='2122';

--TD09
-- Set campaignname = '2122_TD09';
-- SET campaign_start_date = '2021-12-05';
-- SET campaign_end_date = '2022-01-01';
-- SET rolling_date = '2022-01-15';
-- SET td ='TD09';
-- SET year ='2122';

--TD10
-- Set campaignname = '2122_TD10';
-- SET campaign_start_date = '2022-01-02';
-- SET campaign_end_date = '2022-02-05';
-- SET rolling_date = '2022-02-12';
-- SET td ='TD10';
-- SET year ='2122';

-- --TD11
-- Set campaignname = '2122_TD11';
-- SET campaign_start_date = '2022-02-06';
-- SET campaign_end_date = '2022-03-05';
-- SET rolling_date = '2022-03-12';
-- SET td ='TD11';
-- SET year ='2122';

--TD01
-- Set campaignname = '2223_TD01';
-- SET campaign_start_date = '2022-03-06';
-- SET campaign_end_date = '2022-04-09';
-- SET rolling_date = '2022-04-23';
-- SET td ='TD01';
-- SET year ='2223';

--TD02
-- Set campaignname = '2223_TD02';
-- SET campaign_start_date = '2022-04-10';
-- SET campaign_end_date = '2022-05-21';
-- SET rolling_date = '2022-06-04';
-- SET td ='TD02';
-- SET year ='2223';

--TD03
-- Set campaignname = '2223_TD03';
-- SET campaign_start_date = '2022-05-22';
-- SET campaign_end_date = '2022-06-25';
-- SET rolling_date = '2022-07-02';
-- SET td ='TD03';
-- SET year ='2223';

--TD04
-- Set campaignname = '2223_TD04';
-- SET campaign_start_date = '2022-06-26';
-- SET campaign_end_date = '2022-07-23';
-- SET rolling_date = '2022-07-30';
-- SET td ='TD04';
-- SET year ='2223';

--TD05
-- Set campaignname = '2223_TD05';
-- SET campaign_start_date = '2022-07-24';
-- SET campaign_end_date = '2022-08-20';
-- SET rolling_date = '2022-08-27';
-- SET td ='TD05';
-- SET year ='2223';

--TD06
-- Set campaignname = '2223_TD06';
-- SET campaign_start_date = '2022-08-21';
-- SET campaign_end_date = '2022-09-17';
-- SET rolling_date = '2022-09-24';
-- SET td ='TD06';
-- SET year ='2223';


// TABLE NAMES //
SET TD_Date_And_Campaign_Map = concat($td,'_',$year,'_TD_Date_And_Campaign_Map');
SET Barcodes1 = concat($td,'_',$year,'_Barcodes1');
SET Barcodes2 = concat($td,'_',$year,'_Barcodes2');
SET CAT_Redemption1 = concat($td,'_',$year,'_CAT_Redemption1');
SET CAT_Redemption2 = concat($td,'_',$year,'_CAT_Redemption2');
SET CAT_Redemption3 = concat($td,'_',$year,'_CAT_Redemption3');
SET CAT_Redemption4 = concat($td,'_',$year,'_CAT_Redemption4');
SET Prints1 = concat($td,'_',$year,'_Prints1');
SET Prints2 = concat($td,'_',$year,'_Prints2');
SET Prints3 = concat($td,'_',$year,'_Prints3');
SET Prints4 = concat($td,'_',$year,'_Prints4');
SET MO_Payment = concat($td,'_',$year,'_MO_Payment');
SET DM_Redemptions1 = concat($td,'_',$year,'_DM_Redemptions1');
SET DM_Redemptions2 = concat($td,'_',$year,'_DM_Redemptions2');
SET DM_Redemptions3 = concat($td,'_',$year,'_DM_Redemptions3');
SET MO_DM_Redemptions = concat($td,'_',$year,'_MO_DM_Redemptions');
SET Fuel_DM_Redemptions = concat($td,'_',$year,'_Fuel_DM_Redemptions');
SET Fuel_DM_Redemptions1 = concat($td,'_',$year,'_Fuel_DM_Redemptions1');
SET Fuel_DM_Redemptions2 = concat($td,'_',$year,'_Fuel_DM_Redemptions2');
SET selectionfile = concat($td,'_',$year,'_Petrol_DM');
SET CAT_Redemption5 = concat($td,'_',$year,'_CAT_Redemption5');
SET DM_Redemptions4 = concat($td,'_',$year,'_DM_Redemptions4');
SET Fuel_DM_Redemptions3 = concat($td,'_',$year,'_Fuel_DM_Redemptions3');
-- set xxxxx table = concat('CUSTOMER_ANALYTICS.TD_REPORTING.',$td,'_',$year,'_xxxxxxxx')

-- Create my own date and campaign map, normal one is live, this is just for this campaign
create or replace table identifier ($TD_Date_And_Campaign_Map) as
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
               where a.week_start <= $campaign_start_date
                 and a.week_end >= $campaign_start_date
                 and campaign_type = 'Petrol_DM'
                 and (b.redeeming = 1 or b.printing = 1);



--creating barcode lookup, all DM barcodes for money off and fuel weeks for each threshold1
create or replace table identifier ($Barcodes1) as
Select
a.flowchartname,
a.campaignname,
a.flowchartid,
c.barcode,
c.offer_cell,
substring(c.barcode,1,12) as coupon,
     c.reward_value_penceoff,
       c.offer_effective_date,
       c.offer_expiration_date
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
where a.campaignname = $campaignname
and a.flowchartname = 'Petrol_DM'
group by 1,2,3,4,5,6,7,8,9;

select distinct barcode,offer_effective_date,offer_expiration_date from identifier ($Barcodes1)
where offer_cell like 'SPET%'
order by offer_effective_date;





-- add petrol barcode (6p) and start, end and rolling date
Create or replace table identifier ($Barcodes2) as
Select
*,
case when offer_cell like '%SPET%' then '0000004389064' else null end as Petrol_barcode,
case when offer_cell like '%SPET%' then $Rolling_date else null end as petrol_cat_end_date,
$campaign_start_date as campaign_start_date,
$campaign_end_date as campaign_end_date,
'14' rolling_days_validity
from identifier ($Barcodes1);

select * from identifier ($Barcodes2)
where offer_cell like 'SPET%';

--For TD01 where there is 4p vs 7p test
-- Create or replace table identifier ($Barcodes2) as
-- Select
-- *,
-- case when offer_cell like '%SPET%' then '2022-04-23' else null end as petrol_cat_end_date,
-- '2022-03-06' as campaign_start_date,
-- '2022-04-03' as campaign_end_date,
-- '14' rolling_days_validity
-- from identifier ($Barcodes1) as a
-- left join petrol_barcode_map as b
-- on a.reward_value_penceoff = b.pence_off
-- ;





select * from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_shopping_transaction limit 5;

select *
from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_SHOPPING_TRANSACTION_LINE a
inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_EAN as b
    on a.ean_key = b.ean_key
inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_SKU_MAP as c
on b.sku_key = c.sku_key
where SKU_desc like 'UN'
limit 5;

-- Create redemptions for fuel weeks
create or replace table identifier ($CAT_Redemption1) as
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
    bl.reward_value_penceoff,
    '' as offer_code,
    null as threshold1,
    0 as online_flag,
    1 as pfs_red,

--     '0.06' as reward_value_penceoff,
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
                                    petrol_cat_end_date,
                            reward_value_penceoff
            from identifier($Barcodes2)
            where petrol_barcode is not null) as bl
    on pl.coupon_id = bl.Petrol_barcode
        and pl.transaction_date between to_date(bl.campaign_start_date) and to_date(bl.petrol_cat_end_date)

where pl.payment_type_code = '003'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,16,17,18;


-- add on week start and end to fuel week redemptions
create or replace table identifier ($CAT_Redemption2) as
select a.*, b.fin_week, b.week_start, b.week_end
from identifier ($CAT_Redemption1) as a
left join TD_date_map as b
where a.transaction_date between b.week_start and b.week_end;

select count (*) from identifier ($CAT_Redemption2);
-- This is all redeemers, including mal redeemers


-- Redeemers just in that campaign
create or replace table identifier ($CAT_Redemption3) as
select * from identifier ($CAT_Redemption2)
where ec_id in (select ec_id from IDENTIFIER($SELECTIONFILE) where target_control_flag = 1);
Select count(*) from identifier ($CAT_Redemption3);
--Redeemers in total (may have some from other campaigns- reuse barcodes)

-- Create prints of the fuel week
create or replace table identifier ($Prints1) as
select distinct pf.party_account_id as sr_id,
                pf.transaction_date,
                pf.transaction_time,
                case when left(cast(sr_id as varchar(15)), 1) = '4' then '04'
                     when left(cast(sr_id as varchar(15)), 1) = '2' then '02' else '99' end as party_account_type_code,
                pf.location,
                bl.petrol_barcode,
                pf.print_qty,
                0 as redeem_qty,
                bl.campaignname,
                bl.reward_value_penceoff
from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_CATALINA_PRINT_FILE  as pf
         inner join identifier($Barcodes2) as bl
                    on substr(pf.barcode,1,7) = substr(bl.petrol_Barcode,7,7)
                    and pf.transaction_date between bl.campaign_start_date and bl.campaign_end_date;

-- add on EC_ID
create or replace table identifier ($Prints2) as
    select  b.enterprise_customer_id as ec_id, a.*
from identifier ($Prints1) as a
left join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_PARTY_ACCOUNT as b
on a.sr_id = b.party_account_id;

select count(*) from identifier ($Prints2);
-- total prints  - this includes mal redeemers


-- only prints from people that are in selection file
Create or replace table identifier ($Prints3) as
    Select * from identifier ($Prints2)
where EC_ID in (select ec_id from IDENTIFIER($SELECTIONFILE) where target_control_flag = 1);
Select count(*) from identifier ($Prints3);


create or replace table identifier ($Prints4) as
    select a.*,
           b.fin_week,
           b.week_start,
           b.week_end,
           c.threshold1,
           c.segment
    from identifier ($Prints3) as a
left join td_date_map as b
    left join IDENTIFIER($SELECTIONFILE) c
    on a.ec_id = c.ec_id
where a.transaction_date between b.week_start and b.week_end;

select * from Td09_2122_CAT_redemption4;

-- Redeemers where redemption is after print date, excluding those from other campaigns
create or replace table identifier($CAT_Redemption4) as
select distinct b.ec_id,
                b.sr_ID,
       b.redeem_qty,
       b.red_date,
       case when b.red_date < a.print_date then 1 else 0 end as unrelated_redemption_flag,
       c.threshold1,
       c.segment,
       b.Payment_value,
       b.fin_week,
       b.transaction_value,
a.reward_value_penceoff

from (select ec_id,
             sum(print_qty) as print_qty,
             min(transaction_date) as print_date,
             fin_week,
             petrol_barcode,
             reward_value_penceoff
from identifier($Prints4) group by 1,4,5,6) as a
left join (select ec_id,
                  sr_ID,
                  redeem_qty as redeem_qty,
                  transaction_date as red_date,
                  payment_value as Payment_value,
                  fin_week,
                  transaction_value
from identifier($CAT_Redemption3)) as b on a.ec_id = b.ec_id
Left join IDENTIFIER($SELECTIONFILE) as c
on b.ec_id = c.ec_id
where unrelated_redemption_flag = 0 and redeem_qty is not null;
Select * from  identifier($CAT_Redemption4);

-- MONEY OFF WEEKS
create or replace temp table identifier($MO_Payment) as
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
create or replace table identifier($DM_Redemptions1) as
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
from identifier($MO_Payment) as pl
         inner join (select * from identifier($Barcodes2)) as bl
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
create or replace table identifier($DM_Redemptions2) as
select * from identifier($DM_Redemptions1)
where ec_id in (select ec_id from IDENTIFIER($SELECTIONFILE) where target_control_flag = 1);
Select count(*) from identifier($DM_Redemptions2);
Select * from identifier($DM_Redemptions2)
-- total redeeemers in this campaign (petrol and money off)

-- add fin week
create or replace table identifier($DM_Redemptions3) as
select a.*, b.fin_week, b.week_start, b.week_end
from identifier($DM_Redemptions2) as a
left join TD_date_map as b
where a.transaction_date between b.week_start and b.week_end;
select * from identifier($DM_Redemptions3);


create or replace table identifier($MO_DM_Redemptions) as
select a.ec_id,
       a.redeem_qty,
       min(a.transaction_date) as red_date,
       a.transaction_value,
       a.fin_week,
       b.threshold1,
       b.Segment,
       a.offer_cell,
       a.payment_value as Payment_value,
       a.transaction_time
From identifier($DM_Redemptions3) as a
Left join IDENTIFIER($SELECTIONFILE) as b
on a.ec_id = b.ec_id
where offer_cell not like 'SPET%'
Group by 1,2,4,5,6,7,8,9,10;
Select * from identifier($MO_DM_Redemptions)
Select count(ec_id) as vol, redeem_qty as redemptions from identifier($MO_DM_Redemptions) group by 2;

-- Fuel DM redemptions
-- Redemptions for just fuel weeks in the selection file
create or replace table identifier($Fuel_DM_Redemptions) as
select a.ec_id,
       a.redeem_qty,
       min(a.transaction_date) as red_date,
       a.transaction_value,
       a.fin_week,
       b.threshold1,
       b.Segment,
       a.offer_cell,
       a.payment_value as Payment_value,
       a.transaction_time
From identifier($DM_Redemptions3) as a
Left join identifier($selectionfile) as b
on a.ec_id = b.ec_id
where offer_cell  like 'SPET%'
Group by 1,2,4,5,6,7,8,9,10;

-- transactional level
create or replace table identifier($Fuel_DM_Redemptions1) as
select  b.ec_id,
       b.redeem_qty,
       b.red_date,
       c.threshold1,
       c.segment,
       b.transaction_value,
       b.fin_week,
       b.transaction_time
from identifier($Fuel_DM_Redemptions) as b
Left join IDENTIFIER($SELECTIONFILE) as c
on b.ec_id = c.ec_id;
select * from identifier($Fuel_DM_Redemptions1);

-- select distinct ec_id, sum(redeem_qty) as vol from identifier($Fuel_DM_Redemptions1) group by 1 having vol > 1 ;


-- customer level
create or replace table identifier($Fuel_DM_Redemptions2) as
    select distinct ec_id,
           threshold1,
           segment,
           sum(redeem_qty) as redeem_qty,
           fin_week,
           red_date,
           transaction_value
from identifier($Fuel_DM_Redemptions1)
group by 1,2,3,5,6,7;




-- DM summary
select Count(distinct ec_id) as vol, fin_week, segment, threshold1
from identifier($Fuel_DM_Redemptions2)
group by 2,3,4;



-- Prints customer level
Select ec_id, sr_id, Count(*) number_of_prints, fin_week, segment, threshold1
      From identifier($Prints4)
    Group by 1,2,4,5,6;

-- prints summary
select Count(distinct ec_id) as vol, fin_week, threshold1, segment
from identifier($Prints4)
group by 2,3,4;
--6961



// FINAL REDEMPTION TABLES //

select * from identifier($Prints4);
select * from identifier($Fuel_DM_Redemptions2);
select * from identifier($CAT_Redemption4);


//QUESTIONS//

// Target Vol //
select count(*) as volume from identifier($selectionfile);
--TD08
select count(*) as volume from TD08_2122_Petrol_DM where target_control_flag = 1;
--TD09
select count(*) as volume from TD09_2122_Petrol_DM where target_control_flag = 1;
--TD10
select count(*) as volume from TD10_2122_Petrol_DM where target_control_flag = 1;
--TD11
select count(*) as volume from TD11_2122_Petrol_DM where target_control_flag = 1;
--TD01
select count(*) as volume from TD01_2223_Petrol_DM where target_control_flag = 1;
--TD02
select count(*) as volume from TD02_2223_Petrol_DM where target_control_flag = 1;
--TD03
select count(*) as volume from TD03_2223_Petrol_DM where target_control_flag = 1;
--TD04
select count(*) as volume from TD04_2223_Petrol_DM where target_control_flag = 1;
--TD05
select count(*) as volume from TD05_2223_Petrol_DM where target_control_flag = 1;
--TD06
select count(*) as volume from TD06_2223_Petrol_DM where target_control_flag = 1;


select segment, count(*) as volume from identifier($selectionfile) group by 1;
--TD08
select segment, count(*) as volume from TD08_2122_Petrol_DM where target_control_flag = 1 group by 1 order by segment asc;;
--TD09
select segment,  count(*) as volume from TD09_2122_Petrol_DM where target_control_flag = 1 group by 1 order by segment asc;
--TD10
select segment, count(*) as volume from TD10_2122_Petrol_DM  where target_control_flag = 1 group by 1 order by segment asc;;
--TD11
select segment, count(*) as volume from TD11_2122_Petrol_DM where target_control_flag = 1 group by 1 order by segment asc;;
--TD01
select segment, count(*) as volume from TD01_2223_Petrol_DM where target_control_flag = 1 group by 1 order by segment asc;;
--TD02
select segment, count(*) as volume from TD02_2223_Petrol_DM where target_control_flag = 1 group by 1 order by segment asc;;
--TD03
select segment, count(*) as volume from TD03_2223_Petrol_DM where target_control_flag = 1 group by 1 order by segment asc;;
--TD04
select segment, count(*) as volume from TD04_2223_Petrol_DM where target_control_flag = 1 group by 1 order by segment asc;;
--TD05
select segment, count(*) as volume from TD05_2223_Petrol_DM where target_control_flag = 1 group by 1 order by segment asc;;
--TD06
select segment, count(*) as volume from TD06_2223_Petrol_DM where target_control_flag = 1 group by 1 order by segment asc;;

//By threshold1//
select threshold1, count(*) as volume from identifier($selectionfile) group by 1 order by threshold1 asc;
--TD08
select threshold1, count(*) as volume from TD08_2122_Petrol_DM where target_control_flag = 1 group by 1 order by threshold1 asc;
--TD09
select threshold1, count(*) as volume from TD09_2122_Petrol_DM where target_control_flag = 1 group by 1 order by threshold1 asc;
--TD10
select threshold1, count(*) as volume from TD10_2122_Petrol_DM where target_control_flag = 1 group by 1 order by threshold1 asc;
--TD11
select threshold11, count(*) as volume from TD11_2122_Petrol_DM where target_control_flag = 1 group by 1 order by threshold11 asc;
--TD01
select threshold1, count(*) as volume from TD01_2223_Petrol_DM where target_control_flag = 1 group by 1 order by threshold1 asc;
--TD02
select threshold1, count(*) as volume from TD02_2223_Petrol_DM where target_control_flag = 1 group by 1 order by threshold1 asc;
--TD03
select threshold1, count(*) as volume from TD03_2223_Petrol_DM where target_control_flag = 1 group by 1 order by threshold1 asc;
--TD04
select threshold1, count(*) as volume from TD04_2223_Petrol_DM where target_control_flag = 1 group by 1 order by threshold1 asc;
--TD05
select threshold1, count(*) as volume from TD05_2223_Petrol_DM where target_control_flag = 1 group by 1 order by threshold1 asc;
--TD06
select threshold1, count(*) as volume from TD06_2223_Petrol_DM where target_control_flag = 1 group by 1 order by threshold1 asc;



// CAT redemptions //
-- total no. of CAT redemptions
select sum(redeem_qty), segment as vol from identifier($CAT_Redemption4) group by 2 order by segment asc;;
--TD08
select sum(redeem_qty), segment as vol from TD08_2122_CAT_Redemption4 group by 2 order by segment asc;;
--TD09
select sum(redeem_qty), segment as vol from TD09_2122_CAT_Redemption4 group by 2 order by segment asc;;
--TD10
select sum(redeem_qty), segment as vol from TD10_2122_CAT_Redemption4 group by 2 order by segment asc;;
--TD11
select sum(redeem_qty), segment as vol from TD11_2122_CAT_Redemption4 group by 2 order by segment asc;;
--TD01
select sum(redeem_qty), segment as vol from TD01_2223_CAT_Redemption4 group by 2 order by segment asc;;
--TD02
select sum(redeem_qty), segment as vol from TD02_2223_CAT_Redemption4 group by 2 order by segment asc;;
--TD03
select sum(redeem_qty), segment as vol from TD03_2223_CAT_Redemption4 group by 2 order by segment asc;;
--TD04
select sum(redeem_qty), segment as vol from TD04_2223_CAT_Redemption4 group by 2 order by segment asc;;
--TD05
select sum(redeem_qty), segment as vol from TD05_2223_CAT_Redemption4 group by 2 order by segment asc;;
--TD06
select sum(redeem_qty), segment as vol from TD06_2223_CAT_Redemption4 group by 2 order by segment asc;;


select sum(redeem_qty), reward_value_penceoff as vol from TD01_2223_CAT_Redemption4 group by 2;



-- total no. of CAT redeemers
select count(ec_id), segment as vol from identifier($CAT_Redemption4) group by 2 order by segment asc;;
--TD08
select count(distinct ec_id), segment as vol from TD08_2122_CAT_Redemption4 group by 2 order by segment asc;;
--TD09
select count(distinct ec_id), segment as vol from TD09_2122_CAT_Redemption4 group by 2 order by segment asc;;
--TD10
select count(distinct ec_id), segment as vol from TD10_2122_CAT_Redemption4 group by 2 order by segment asc;;
--TD11
select count(distinct ec_id), segment as vol from TD11_2122_CAT_Redemption4 group by 2 order by segment asc;;
--TD01
select count(distinct ec_id), segment as vol from TD01_2223_CAT_Redemption4 group by 2 order by segment asc;;
--TD02
select count(distinct ec_id), segment as vol from TD02_2223_CAT_Redemption4 group by 2 order by segment asc;;
--TD03
select count(distinct ec_id), segment as vol from TD03_2223_CAT_Redemption4 group by 2 order by segment asc;;
--TD04
select count(distinct ec_id), segment as vol from TD04_2223_CAT_Redemption4 group by 2 order by segment asc;;
--TD05
select count(distinct ec_id), segment as vol from TD05_2223_CAT_Redemption4 group by 2 order by segment asc;;
--TD06
select count(distinct ec_id), segment as vol from TD06_2223_CAT_Redemption4 group by 2 order by segment asc;;


-- total no. of CAT redeemers vs redemptions
select count (distinct ec_id), sum(redeem_qty) from identifier($CAT_Redemption4);

-- no. of CAT redemptions by number
Select distinct count(ec_id) as volume, Number_of_CAT_redemptions
      From (select ec_id, sum(redeem_qty) as Number_of_CAT_redemptions
      from identifier($CAT_Redemption4) group by 1)
group by 2
order by Number_of_CAT_redemptions asc;

--TD08
Select distinct count(ec_id) as volume, Number_of_CAT_redemptions
      From (select ec_id, sum(redeem_qty) as Number_of_CAT_redemptions
      from TD08_2122_CAT_Redemption4 group by 1)
group by 2
order by Number_of_CAT_redemptions asc;

--TD09
Select distinct count(ec_id) as volume, Number_of_CAT_redemptions
      From (select ec_id, sum(redeem_qty) as Number_of_CAT_redemptions
      from TD09_2122_CAT_Redemption4 group by 1)
group by 2
order by Number_of_CAT_redemptions asc;

--TD10
Select distinct count(ec_id) as volume, Number_of_CAT_redemptions
      From (select ec_id, sum(redeem_qty) as Number_of_CAT_redemptions
      from TD10_2122_CAT_Redemption4 group by 1)
group by 2
order by Number_of_CAT_redemptions asc;

--TD11
Select distinct count(ec_id) as volume, Number_of_CAT_redemptions
      From (select ec_id, sum(redeem_qty) as Number_of_CAT_redemptions
      from TD11_2122_CAT_Redemption4 group by 1)
group by 2
order by Number_of_CAT_redemptions asc;

--TD01
Select distinct count(ec_id) as volume, Number_of_CAT_redemptions
      From (select ec_id, sum(redeem_qty) as Number_of_CAT_redemptions
      from TD01_2223_CAT_Redemption4 group by 1)
group by 2
order by Number_of_CAT_redemptions asc;

--TD02
Select distinct count(ec_id) as volume, Number_of_CAT_redemptions
      From (select ec_id, sum(redeem_qty) as Number_of_CAT_redemptions
      from TD02_2223_CAT_Redemption4 group by 1)
group by 2
order by Number_of_CAT_redemptions asc;

--TD03
Select distinct count(ec_id) as volume, Number_of_CAT_redemptions
      From (select ec_id, sum(redeem_qty) as Number_of_CAT_redemptions
      from TD03_2223_CAT_Redemption4 group by 1)
group by 2
order by Number_of_CAT_redemptions asc;

--TD04
Select distinct count(ec_id) as volume, Number_of_CAT_redemptions
      From (select ec_id, sum(redeem_qty) as Number_of_CAT_redemptions
      from TD04_2223_CAT_Redemption4 group by 1)
group by 2
order by Number_of_CAT_redemptions asc;

--TD05
Select distinct count(ec_id) as volume, Number_of_CAT_redemptions
      From (select ec_id, sum(redeem_qty) as Number_of_CAT_redemptions
      from TD05_2223_CAT_Redemption4 group by 1)
group by 2
order by Number_of_CAT_redemptions asc;

--TD06
Select distinct count(ec_id) as volume, Number_of_CAT_redemptions
      From (select ec_id, sum(redeem_qty) as Number_of_CAT_redemptions
      from TD06_2223_CAT_Redemption4 group by 1)
group by 2
order by Number_of_CAT_redemptions asc;



-- -- no. of CAT redemptions per redeemer
-- Select distinct ec_id, Number_of_CAT_redemptions
--       From (select ec_id, count(*) as Number_of_CAT_redemptions
--       from identifier($CAT_Redemption4) group by 1)
-- group by 1,2;

 //Engagement by day
-- CAT
select sum(redeem_qty) as volume, red_date from identifier($CAT_Redemption4) group by 2;
--TD08
 select sum(redeem_qty) as volume, red_date from TD08_2122_CAT_Redemption4 group by 2;
--TD09
 select sum(redeem_qty) as volume, red_date from TD09_2122_CAT_Redemption4 group by 2;
--TD10
 select sum(redeem_qty) as volume, red_date from TD10_2122_CAT_Redemption4 group by 2;
--TD11
 select sum(redeem_qty) as volume, red_date from TD11_2122_CAT_Redemption4 group by 2;
--TD01
 select sum(redeem_qty) as volume, red_date from TD01_2223_CAT_Redemption4 group by 2;
--TD02
 select sum(redeem_qty) as volume, red_date from TD02_2223_CAT_Redemption4 group by 2;
--TD03
 select sum(redeem_qty) as volume, red_date from TD03_2223_CAT_Redemption4 group by 2;
--TD04
 select sum(redeem_qty) as volume, red_date from TD04_2223_CAT_Redemption4 group by 2;
--TD05
 select sum(redeem_qty) as volume, red_date from TD05_2223_CAT_Redemption4 group by 2;
--TD06
 select sum(redeem_qty) as volume, red_date  from TD06_2223_CAT_Redemption4 group by 2;




// DM redemptions //

-- total no. of DM redemptions
select sum(redeem_qty) as vol, segment from identifier($Fuel_DM_Redemptions2) group by 2;
--TD08
select sum(redeem_qty) as vol, segment from TD08_2122_Fuel_DM_Redemptions2 group by 2 order by segment asc;

--TD09
select sum(redeem_qty) as vol, segment from TD09_2122_Fuel_DM_Redemptions2 group by 2 order by segment asc;
--TD10
select sum(redeem_qty) as vol, segment from TD10_2122_Fuel_DM_Redemptions2 group by 2 order by segment asc;
--TD11
select sum(redeem_qty) as vol, segment from TD11_2122_Fuel_DM_Redemptions2 group by 2 order by segment asc;
--TD01
select sum(redeem_qty) as vol, segment from TD01_2223_Fuel_DM_Redemptions2 group by 2 order by segment asc;
--TD02
select sum(redeem_qty) as vol, segment from TD02_2223_Fuel_DM_Redemptions2 group by 2 order by segment asc;
--TD03
select sum(redeem_qty) as vol, segment from TD03_2223_Fuel_DM_Redemptions2 group by 2 order by segment asc;
--TD04
select sum(redeem_qty) as vol, segment from TD04_2223_Fuel_DM_Redemptions2 group by 2 order by segment asc;
--TD05
select sum(redeem_qty) as vol, segment from TD05_2223_Fuel_DM_Redemptions2 group by 2 order by segment asc;
--TD06
select sum(redeem_qty) as vol, segment from TD06_2223_Fuel_DM_Redemptions2 group by 2 order by segment asc;



-- total no. of DM redeemers

--TD08
select count(distinct ec_id), segment as vol from TD08_2122_Fuel_DM_Redemptions2 group by 2 order by segment asc;;
--TD09
select count(distinct ec_id), segment as vol from TD09_2122_Fuel_DM_Redemptions2 group by 2 order by segment asc;;
--TD10
select count(distinct ec_id), segment as vol from TD10_2122_Fuel_DM_Redemptions2 group by 2 order by segment asc;;
--TD11
select count(distinct ec_id), segment as vol from TD11_2122_Fuel_DM_Redemptions2 group by 2 order by segment asc;;
--TD01
select count(distinct ec_id), segment as vol from TD01_2223_Fuel_DM_Redemptions2 group by 2 order by segment asc;;
--TD02
select count(distinct ec_id), segment as vol from TD02_2223_Fuel_DM_Redemptions2 group by 2 order by segment asc;;
--TD03
select count(distinct ec_id), segment as vol from TD03_2223_Fuel_DM_Redemptions2 group by 2 order by segment asc;;
--TD04
select count(distinct ec_id), segment as vol from TD04_2223_Fuel_DM_Redemptions2 group by 2 order by segment asc;;
--TD05
select count(distinct ec_id), segment as vol from TD05_2223_Fuel_DM_Redemptions2 group by 2 order by segment asc;;
--TD06
select count(distinct ec_id), segment as vol from TD06_2223_Fuel_DM_Redemptions2 group by 2 order by segment asc;;




-- no. of DM redemptions by number
Select distinct count(ec_id) as volume, Number_of_DM_redemptions
      From (select ec_id, count(redeem_qty) as Number_of_DM_redemptions
      from identifier($Fuel_DM_Redemptions2) group by 1)
    Group by 2;
--TD08
Select distinct count(ec_id), Number_of_DM_redemptions
      From (select ec_id, sum(redeem_qty) as Number_of_DM_redemptions
      from TD08_2122_Fuel_DM_Redemptions2 group by 1)
    Group by 2
order by Number_of_DM_redemptions asc;

--TD09
Select distinct count(ec_id) as volume, Number_of_DM_redemptions
      From (select ec_id, sum(redeem_qty) as Number_of_DM_redemptions
      from TD09_2122_Fuel_DM_Redemptions2 group by 1)
    Group by 2
order by Number_of_DM_redemptions asc;

--TD10
Select distinct count(ec_id) as volume, Number_of_DM_redemptions
      From (select ec_id, sum(redeem_qty) as Number_of_DM_redemptions
      from TD10_2122_Fuel_DM_Redemptions2 group by 1)
    Group by 2
order by Number_of_DM_redemptions asc;
--TD11
Select distinct count(ec_id) as volume, Number_of_DM_redemptions
      From (select ec_id, sum(redeem_qty) as Number_of_DM_redemptions
      from TD11_2122_Fuel_DM_Redemptions2 group by 1)
    Group by 2
order by Number_of_DM_redemptions asc;
--TD01
Select distinct count(ec_id) as volume, Number_of_DM_redemptions
      From (select ec_id, sum(redeem_qty) as Number_of_DM_redemptions
      from TD01_2223_Fuel_DM_Redemptions2 group by 1)
    Group by 2
order by Number_of_DM_redemptions asc;
--TD02
Select distinct count(ec_id) as volume, Number_of_DM_redemptions
      From (select ec_id, sum(redeem_qty) as Number_of_DM_redemptions
      from TD02_2223_Fuel_DM_Redemptions2 group by 1)
    Group by 2
order by Number_of_DM_redemptions asc;
--TD03
Select distinct count(ec_id) as volume, Number_of_DM_redemptions
      From (select ec_id, sum(redeem_qty) as Number_of_DM_redemptions
      from TD03_2223_Fuel_DM_Redemptions2 group by 1)
    Group by 2
order by Number_of_DM_redemptions asc;
--TD04
Select distinct count(ec_id) as volume, Number_of_DM_redemptions
      From (select ec_id, sum(redeem_qty) as Number_of_DM_redemptions
      from TD04_2223_Fuel_DM_Redemptions2 group by 1)
    Group by 2
order by Number_of_DM_redemptions asc;
--TD05
Select distinct count(ec_id) as volume, Number_of_DM_redemptions
      From (select ec_id, sum(redeem_qty) as Number_of_DM_redemptions
      from TD05_2223_Fuel_DM_Redemptions2 group by 1)
    Group by 2
order by Number_of_DM_redemptions asc;
--TD06
Select distinct count(ec_id) as volume, Number_of_DM_redemptions
      From (select ec_id,sum(redeem_qty) as Number_of_DM_redemptions
      from TD06_2223_Fuel_DM_Redemptions2 group by 1)
    Group by 2
order by Number_of_DM_redemptions asc;

//Engagement by week (DM redemptions)
--  select count (*) as volume, fin_week from identifier($CAT_Redemption4) group by 2;
--  select count (*) as volume, fin_week from identifier($Fuel_DM_Redemptions2) group by 2;
--  select count (*) as volume, fin_week from identifier($Prints4) group by 2;
 select count (*) as volume, fin_week from identifier($Fuel_DM_Redemptions2) group by 2;

--TD08
 select count (*) as volume, fin_week from TD08_2122_Fuel_DM_Redemptions2 group by 2 order by fin_week asc;
 select sum(redeem_qty) as volume, fin_week from TD08_2122_Fuel_DM_Redemptions2 group by 2 order by fin_week asc;
--TD09
 select count (distinct ec_id) as volume, fin_week from TD09_2122_Fuel_DM_Redemptions2 group by 2 order by fin_week asc;
 select sum(redeem_qty) as volume, fin_week from TD09_2122_Fuel_DM_Redemptions2 group by 2 order by fin_week asc;
--TD10
 select count (*) as volume, fin_week from TD10_2122_Fuel_DM_Redemptions2 group by 2 order by fin_week asc;
 select sum(redeem_qty) as volume, fin_week from TD10_2122_Fuel_DM_Redemptions2 group by 2 order by fin_week asc;
--TD11
 select count (*) as volume, fin_week from TD11_2122_Fuel_DM_Redemptions2 group by 2 order by fin_week asc;
 select sum(redeem_qty) as volume, fin_week from TD11_2122_Fuel_DM_Redemptions2 group by 2 order by fin_week asc;
--TD01
 select count (*) as volume, fin_week from TD01_2223_Fuel_DM_Redemptions2 group by 2 order by fin_week asc;
 select sum(redeem_qty) as volume, fin_week from TD01_2223_Fuel_DM_Redemptions2 group by 2 order by fin_week asc;
--TD02
 select count (*) as volume, fin_week from TD02_2223_Fuel_DM_Redemptions2 group by 2 order by fin_week asc;
 select sum(redeem_qty) as volume, fin_week from TD02_2223_Fuel_DM_Redemptions2 group by 2 order by fin_week asc;
--TD03
 select count (*) as volume, fin_week from TD03_2223_Fuel_DM_Redemptions2 group by 2 order by fin_week asc;
 select sum(redeem_qty) as volume, fin_week from TD03_2223_Fuel_DM_Redemptions2 group by 2 order by fin_week asc;
--TD04
 select count (*) as volume, fin_week from TD04_2223_Fuel_DM_Redemptions2 group by 2 order by fin_week asc;
 select sum(redeem_qty) as volume, fin_week from TD04_2223_Fuel_DM_Redemptions2 group by 2 order by fin_week asc;
--TD05
 select count (*) as volume, fin_week from TD05_2223_Fuel_DM_Redemptions2 group by 2 order by fin_week asc;
 select sum(redeem_qty) as volume, fin_week from TD05_2223_Fuel_DM_Redemptions2 group by 2 order by fin_week asc;
--TD06
 select count (*) as volume, fin_week from TD06_2223_Fuel_DM_Redemptions2 group by 2 order by fin_week asc;
 select sum(redeem_qty) as volume, fin_week from TD06_2223_Fuel_DM_Redemptions2 group by 2 order by fin_week asc;


// Engagement by day //
select sum(redeem_qty) as volume,red_date from identifier($Fuel_DM_Redemptions2) group by 2;
--TD08
 select sum(redeem_qty) as volume, red_date from TD08_2122_Fuel_DM_Redemptions2 group by 2;
--TD09
 select sum(redeem_qty) as volume, red_date from TD09_2122_Fuel_DM_Redemptions2 group by 2;
--TD10
 select sum(redeem_qty) as volume, red_date from TD10_2122_Fuel_DM_Redemptions2 group by 2;
--TD11
 select sum(redeem_qty) as volume, red_date from TD11_2122_Fuel_DM_Redemptions2 group by 2;
--TD01
 select sum(redeem_qty) as volume, red_date from TD01_2223_Fuel_DM_Redemptions2 group by 2;
--TD02
 select sum(redeem_qty) as volume, red_date from TD02_2223_Fuel_DM_Redemptions2 group by 2;
--TD03
 select sum(redeem_qty) as volume, red_date from TD03_2223_Fuel_DM_Redemptions2 group by 2;
--TD04
 select sum(redeem_qty) as volume, red_date from TD04_2223_Fuel_DM_Redemptions2 group by 2;
--TD05
 select sum(redeem_qty) as volume, red_date from TD05_2223_Fuel_DM_Redemptions2 group by 2;
--TD06
 select sum(redeem_qty) as volume, red_date  from TD06_2223_Fuel_DM_Redemptions2 group by 2;


// PRINTS //

-- NOTE: all excess prints are accounted for in the back up file
-- Total no. of prints
select count (*),segment as vol from identifier($Prints4) group by 2;
select count (*),segment as vol from TD08_2122_Prints4 group by 2 order by vol asc;
select count (*),segment as vol from TD09_2122_Prints4 group by 2 order by vol asc;
select count (*),segment as vol from TD10_2122_Prints4 group by 2 order by vol asc;
select count (*),segment as vol from TD11_2122_Prints4 group by 2 order by vol asc;
select count (*),segment as vol from TD01_2223_Prints4 group by 2 order by vol asc;
select count (*),segment as vol from TD02_2223_Prints4 group by 2 order by vol asc;
select count (*),segment as vol from TD03_2223_Prints4 group by 2 order by vol asc;
select count (*),segment as vol from TD04_2223_Prints4 group by 2 order by vol asc;
select count (*),segment as vol from TD05_2223_Prints4 group by 2 order by vol asc;
select count (*),segment as vol from TD06_2223_Prints4 group by 2 order by vol asc;

-- no. of prints by number
Select distinct count(ec_id) as volume, number_of_prints
      From (select ec_id, sum(print_qty) as number_of_prints
      from identifier($Prints4)
      group by 1)
    Group by 2
order by number_of_prints asc;

--TD08
Select distinct count(ec_id) as volume, number_of_prints
      From (select ec_id, count(*) as number_of_prints
      from TD08_2122_Prints4
      group by 1)
    Group by 2
order by number_of_prints asc;

--TD09
Select distinct count(ec_id) as volume, number_of_prints
      From (select ec_id, count(*) as number_of_prints
      from TD09_2122_Prints4
      group by 1)
    Group by 2
order by number_of_prints asc;

--TD10
Select distinct count(ec_id) as volume, number_of_prints
      From (select ec_id, count(*) as number_of_prints
      from TD10_2122_Prints4
      group by 1)
    Group by 2
order by number_of_prints asc;

--TD11
Select distinct count(ec_id) as volume, number_of_prints
      From (select ec_id, count(*) as number_of_prints
      from TD11_2122_Prints4
      group by 1)
    Group by 2
order by number_of_prints asc;

--TD01
Select distinct count(ec_id) as volume, number_of_prints
      From (select ec_id, count(*) as number_of_prints
      from TD01_2223_Prints4
      group by 1)
    Group by 2
order by number_of_prints asc;

--TD02
Select distinct count(ec_id) as volume, number_of_prints
      From (select ec_id, count(*) as number_of_prints
      from TD02_2223_Prints4
      group by 1)
    Group by 2
order by number_of_prints asc;

--TD03
Select distinct count(ec_id) as volume, number_of_prints
      From (select ec_id, count(*) as number_of_prints
      from TD03_2223_Prints4
      group by 1)
    Group by 2
order by number_of_prints asc;

--TD04
Select distinct count(ec_id) as volume, number_of_prints
      From (select ec_id, count(*) as number_of_prints
      from TD04_2223_Prints4
      group by 1)
    Group by 2
order by number_of_prints asc;

--TD05
Select distinct count(ec_id) as volume, number_of_prints
      From (select ec_id, count(*) as number_of_prints
      from TD05_2223_Prints4
      group by 1)
    Group by 2
order by number_of_prints asc;

--TD06
Select distinct count(ec_id) as volume, number_of_prints
      From (select ec_id, count(*) as number_of_prints
      from TD06_2223_Prints4
      group by 1)
    Group by 2
order by number_of_prints asc;




-- // how much do customers spend on fuel //
-- select avg(transaction_value) as average_spend, segment from identifier($CAT_Redemption4) group by 2;
-- select avg(transaction_value) as average_spend, threshold1 from identifier($CAT_Redemption4) group by 2;
--
-- select avg(transaction_value) as average_spend, segment, threshold1 from identifier($CAT_Redemption4) group by 2;
-- --TD08
--  select avg(transaction_value) as average_spend, segment from TD08_2122_CAT_Redemption4 group by 2;
-- --TD09
--  select avg(transaction_value) as average_spend, segment from TD09_2122_CAT_Redemption4 group by 2;
-- --TD10
--  select avg(transaction_value) as average_spend, segment from TD10_2122_CAT_Redemption4 group by 2;
-- --TD11
--  select avg(transaction_value) as average_spend, segment from TD11_2122_CAT_Redemption4 group by 2;
-- --TD01
--  select avg(transaction_value) as average_spend, segment from TD01_2223_CAT_Redemption4 group by 2;
-- --TD02
--  select avg(transaction_value) as average_spend, segment from TD02_2223_CAT_Redemption4 group by 2;
-- --TD03
--  select avg(transaction_value) as average_spend, segment from TD03_2223_CAT_Redemption4 group by 2;
-- --TD04
--  select avg(transaction_value) as average_spend, segment from TD04_2223_CAT_Redemption4 group by 2;
-- --TD05
--  select avg(transaction_value) as average_spend, segment from TD05_2223_CAT_Redemption4 group by 2;
-- --TD06
--  select avg(transaction_value) as average_spend, segment from TD06_2223_CAT_Redemption4 group by 2;
--
-- //By threshold1 //
-- select avg(transaction_value) as average_spend, threshold1 from identifier($CAT_Redemption4) group by 2;
-- --TD08
--  select avg(transaction_value) as average_spend, threshold1 from TD08_2122_CAT_Redemption4 group by 2 order by threshold1 asc;
-- --TD09
--  select avg(transaction_value) as average_spend, threshold1 from TD09_2122_CAT_Redemption4 group by 2 order by threshold1 asc;
-- --TD10
--  select avg(transaction_value) as average_spend, threshold1 from TD10_2122_CAT_Redemption4 group by 2 order by threshold1 asc;
-- --TD11
--  select avg(transaction_value) as average_spend, threshold1 from TD11_2122_CAT_Redemption4 group by 2 order by threshold1 asc;
-- --TD01
--  select avg(transaction_value) as average_spend, threshold1 from TD01_2223_CAT_Redemption4 group by 2 order by threshold1 asc;
-- --TD02
--  select avg(transaction_value) as average_spend, threshold1 from TD02_2223_CAT_Redemption4 group by 2 order by threshold1 asc;
-- --TD03
--  select avg(transaction_value) as average_spend, threshold1 from TD03_2223_CAT_Redemption4 group by 2 order by threshold1 asc;
-- --TD04
--  select avg(transaction_value) as average_spend, threshold1 from TD04_2223_CAT_Redemption4 group by 2 order by threshold1 asc;
-- --TD05
--  select avg(transaction_value) as average_spend, threshold1 from TD05_2223_CAT_Redemption4 group by 2 order by threshold1 asc;
-- --TD06
--  select avg(transaction_value) as average_spend, threshold1  from TD06_2223_CAT_Redemption4 group by 2 order by threshold1 asc;


// How much do customers spend in shop to meet threshold1//
select avg(transaction_value) as average_spend, segment from identifier($Fuel_DM_Redemptions2) group by 2;
select threshold1, avg(transaction_value) as average_spend  from identifier($Fuel_DM_Redemptions2) group by 1 order by threshold1 ASC;

--TD08
 select avg(transaction_value) as average_spend, segment from TD08_2122_Fuel_DM_Redemptions2 group by 2 order by segment asc;;
--TD09
 select avg(transaction_value) as average_spend, segment from TD09_2122_Fuel_DM_Redemptions2 group by 2 order by segment asc;;
--TD10
 select avg(transaction_value) as average_spend, segment from TD10_2122_Fuel_DM_Redemptions2 group by 2 order by segment asc;;
--TD11
 select avg(transaction_value) as average_spend, segment from TD11_2122_Fuel_DM_Redemptions2 group by 2 order by segment asc;;
--TD01
 select avg(transaction_value) as average_spend, segment from TD01_2223_Fuel_DM_Redemptions2 group by 2 order by segment asc;;
--TD02
 select avg(transaction_value) as average_spend, segment from TD02_2223_Fuel_DM_Redemptions2 group by 2 order by segment asc;;
--TD03
 select avg(transaction_value) as average_spend, segment from TD03_2223_Fuel_DM_Redemptions2 group by 2 order by segment asc;;
--TD04
 select avg(transaction_value) as average_spend, segment from TD04_2223_Fuel_DM_Redemptions2 group by 2 order by segment asc;;
--TD05
 select avg(transaction_value) as average_spend, segment from TD05_2223_Fuel_DM_Redemptions2 group by 2 order by segment asc;;
--TD06
 select avg(transaction_value) as average_spend, segment from TD06_2223_Fuel_DM_Redemptions2 group by 2 order by segment asc;;

//By threshold1 //
--TD08
 select avg(transaction_value) as average_spend, threshold1 from TD08_2122_Fuel_DM_Redemptions2 group by 2 order by threshold1 asc;
--TD09
 select avg(transaction_value) as average_spend, threshold1 from TD09_2122_Fuel_DM_Redemptions2 group by 2 order by threshold1 asc;
--TD10
 select avg(transaction_value) as average_spend, threshold1 from TD10_2122_Fuel_DM_Redemptions2 group by 2 order by threshold1 asc;
--TD11
 select avg(transaction_value) as average_spend, threshold1 from TD11_2122_Fuel_DM_Redemptions2 group by 2 order by threshold1 asc;
--TD01
 select avg(transaction_value) as average_spend, threshold1 from TD01_2223_Fuel_DM_Redemptions2 group by 2 order by threshold1 asc;
--TD02
 select avg(transaction_value) as average_spend, threshold1 from TD02_2223_Fuel_DM_Redemptions2 group by 2 order by threshold1 asc;
--TD03
 select avg(transaction_value) as average_spend, threshold1 from TD03_2223_Fuel_DM_Redemptions2 group by 2 order by threshold1 asc;
--TD04
 select avg(transaction_value) as average_spend, threshold1 from TD04_2223_Fuel_DM_Redemptions2 group by 2 order by threshold1 asc;
--TD05
 select avg(transaction_value) as average_spend, threshold1 from TD05_2223_Fuel_DM_Redemptions2 group by 2 order by threshold1 asc;
--TD06
 select avg(transaction_value) as average_spend, threshold1  from TD06_2223_Fuel_DM_Redemptions2 group by 2 order by threshold1 asc;


// Red rate //
//CAT//
create or replace temp table identifier($CAT_Redemption5) as
select count(a.ec_id) as selection,
       count(distinct b.ec_id) as redeemers,
       sum(b.redeem_qty) as redemptions,
       a.segment as segment
from identifier($selectionfile) as a
left join identifier($CAT_Redemption4) as b
on a.ec_id = b.ec_id
where a.target_control_flag = 1
group by 4;



-- Total participation rate
Select round(((sum(redeemers))/ (sum(selection))) *100,2) as Participation_rate
from identifier($CAT_Redemption5);
--TD08
Select round(((sum(redeemers))/ (sum(selection))) *100,2) as Participation_rate
from TD08_2122_CAT_Redemption5;
--TD09
Select round(((sum(redeemers))/ (sum(selection))) *100,2) as Participation_rate
from TD09_2122_CAT_Redemption5;
--TD10
Select round(((sum(redeemers))/ (sum(selection))) *100,2) as Participation_rate
from TD10_2122_CAT_Redemption5;
--TD11
Select round(((sum(redeemers))/ (sum(selection))) *100,2) as Participation_rate
from TD11_2122_CAT_Redemption5;
--TD01
Select round(((sum(redeemers))/ (sum(selection))) *100,2) as Participation_rate
from TD01_2223_CAT_Redemption5;
--TD02
Select round(((sum(redeemers))/ (sum(selection))) *100,2) as Participation_rate
from TD02_2223_CAT_Redemption5;
--TD03
Select round(((sum(redeemers))/ (sum(selection))) *100,2) as Participation_rate
from TD03_2223_CAT_Redemption5;
--TD04
Select round(((sum(redeemers))/ (sum(selection))) *100,2) as Participation_rate
from TD04_2223_CAT_Redemption5;
--TD05
Select round(((sum(redeemers))/ (sum(selection))) *100,2) as Participation_rate
from TD05_2223_CAT_Redemption5;
--TD06
Select round(((sum(redeemers))/ (sum(selection))) *100,2) as Participation_rate
from TD06_2223_CAT_Redemption5;



-- participation rate by segment
Select segment, round(((sum(redeemers))/ (sum(selection))) *100,2) as Participation_rate
from identifier($CAT_Redemption5)
group by 1;
--TD08
Select segment, round(((sum(redeemers))/ (sum(selection))) *100,2) as Participation_rate
from TD08_2122_CAT_Redemption5
group by 1;
--TD09
Select segment, round(((sum(redeemers))/ (sum(selection))) *100,2) as Participation_rate
from TD09_2122_CAT_Redemption5
group by 1
Order by segment asc;
--TD10
Select segment, round(((sum(redeemers))/ (sum(selection))) *100,2) as Participation_rate
from TD10_2122_CAT_Redemption5
group by 1
Order by segment asc;
--TD11
Select segment, round(((sum(redeemers))/ (sum(selection))) *100,2) as Participation_rate
from TD11_2122_CAT_Redemption5
group by 1
Order by segment asc;
--TD01
Select segment, round(((sum(redeemers))/ (sum(selection))) *100,2) as Participation_rate
from TD01_2223_CAT_Redemption5
group by 1
Order by segment asc;
--TD02
Select segment, round(((sum(redeemers))/ (sum(selection))) *100,2) as Participation_rate
from TD02_2223_CAT_Redemption5
group by 1
Order by segment asc;
--TD03
Select segment, round(((sum(redeemers))/ (sum(selection))) *100,2) as Participation_rate
from TD03_2223_CAT_Redemption5
group by 1
Order by segment asc;
--TD04
Select segment, round(((sum(redeemers))/ (sum(selection))) *100,2) as Participation_rate
from TD04_2223_CAT_Redemption5
group by 1
Order by segment asc;
--TD05
Select segment, round(((sum(redeemers))/ (sum(selection))) *100,2) as Participation_rate
from TD05_2223_CAT_Redemption5
group by 1
Order by segment asc;
--TD06
Select segment, round(((sum(redeemers))/ (sum(selection))) *100,2) as Participation_rate
from TD06_2223_CAT_Redemption5
group by 1
Order by segment asc;


-- Total red rate
Select round(((sum(redemptions))/ (sum(selection))) *100,2) as Redemption_rate
from identifier($CAT_Redemption5);
--TD08
Select round(((sum(redemptions))/ (sum(selection))) *100,2) as Redemption_rate
from TD08_2122_CAT_Redemption5;
--TD09
Select round(((sum(redemptions))/ (sum(selection))) *100,2) as Redemption_rate
from TD09_2122_CAT_Redemption5;
--TD10
Select round(((sum(redemptions))/ (sum(selection))) *100,2) as Redemption_rate
from TD10_2122_CAT_Redemption5;
--TD11
Select round(((sum(redemptions))/ (sum(selection))) *100,2) as Redemption_rate
from TD11_2122_CAT_Redemption5;
--TD01
Select round(((sum(redemptions))/ (sum(selection))) *100,2) as Redemption_rate
from TD01_2223_CAT_Redemption5;
--TD02
Select round(((sum(redemptions))/ (sum(selection))) *100,2) as Redemption_rate
from TD02_2223_CAT_Redemption5;
--TD03
Select round(((sum(redemptions))/ (sum(selection))) *100,2) as Redemption_rate
from TD03_2223_CAT_Redemption5;
--TD04
Select round(((sum(redemptions))/ (sum(selection))) *100,2) as Redemption_rate
from TD04_2223_CAT_Redemption5;
--TD05
Select round(((sum(redemptions))/ (sum(selection))) *100,2) as Redemption_rate
from TD05_2223_CAT_Redemption5;
--TD06
Select round(((sum(redemptions))/ (sum(selection))) *100,2) as Redemption_rate
from TD06_2223_CAT_Redemption5;



-- Red rate by segment
Select segment, round(((sum(redemptions))/ (sum(selection))) *100,2) as Redemption_rate
from identifier($CAT_Redemption5)
group by 1
Order by segment asc;
--TD08
Select segment, round(((sum(redemptions))/ (sum(selection))) *100,2) as Redemption_rate
from TD08_2122_CAT_Redemption5
group by 1
Order by segment asc;
--TD09
Select segment, round(((sum(redemptions))/ (sum(selection))) *100,2) as Redemption_rate
from TD09_2122_CAT_Redemption5
group by 1;
--TD10
Select segment, round(((sum(redemptions))/ (sum(selection))) *100,2) as Redemption_rate
from TD10_2122_CAT_Redemption5
group by 1
Order by segment asc;
--TD11
Select segment, round(((sum(redemptions))/ (sum(selection))) *100,2) as Redemption_rate
from TD11_2122_CAT_Redemption5
group by 1
Order by segment asc;
--TD01
Select segment, round(((sum(redemptions))/ (sum(selection))) *100,2) as Redemption_rate
from TD01_2223_CAT_Redemption5
group by 1
Order by segment asc;
--TD02
Select segment, round(((sum(redemptions))/ (sum(selection))) *100,2) as Redemption_rate
from TD02_2223_CAT_Redemption5
group by 1
Order by segment asc;
--TD03
Select segment, round(((sum(redemptions))/ (sum(selection))) *100,2) as Redemption_rate
from TD03_2223_CAT_Redemption5
group by 1
Order by segment asc;
--TD04
Select segment, round(((sum(redemptions))/ (sum(selection))) *100,2) as Redemption_rate
from TD04_2223_CAT_Redemption5
group by 1
Order by segment asc;
--TD05
Select segment, round(((sum(redemptions))/ (sum(selection))) *100,2) as Redemption_rate
from TD05_2223_CAT_Redemption5
group by 1
Order by segment asc;
--TD06
Select segment, round(((sum(redemptions))/ (sum(selection))) *100,2) as Redemption_rate
from TD06_2223_CAT_Redemption5
group by 1
Order by segment asc;





//DM//
create or replace temp table identifier($Fuel_DM_Redemptions3) as
select count(a.ec_id) as selection,
       count(distinct b.ec_id) as redeemers,
       sum(b.redeem_qty) as redemptions,
       a.segment as segment
from identifier($selectionfile) as a
left join identifier($Fuel_DM_Redemptions2) as b
on a.ec_id = b.ec_id
where a.target_control_flag=1
group by 4;


select sum(redemptions) from TD09_2122_Fuel_DM_redemptions3;
select sum(redemptions),segment from TD09_2122_Fuel_DM_redemptions3 group by 2;

--Total participation Rate
select sum(redemptions), sum(redeemers) from identifier($Fuel_DM_Redemptions3);
--TD08
Select round(((sum(redeemers))/ (sum(selection))) *100,2) as Participation_rate
from TD08_2122_Fuel_DM_Redemptions3;
--TD09
Select round(((sum(redeemers))/ (sum(selection))) *100,2) as Participation_rate
from TD09_2122_Fuel_DM_Redemptions3;
--TD10
Select round(((sum(redeemers))/ (sum(selection))) *100,2) as Participation_rate
from TD10_2122_Fuel_DM_Redemptions3;
--TD11
Select round(((sum(redeemers))/ (sum(selection))) *100,2) as Participation_rate
from TD11_2122_Fuel_DM_Redemptions3;
--TD01
Select round(((sum(redeemers))/ (sum(selection))) *100,2) as Participation_rate
from TD01_2223_Fuel_DM_Redemptions3;
--TD02
Select round(((sum(redeemers))/ (sum(selection))) *100,2) as Participation_rate
from TD02_2223_Fuel_DM_Redemptions3;
--TD03
Select round(((sum(redeemers))/ (sum(selection))) *100,2) as Participation_rate
from TD03_2223_Fuel_DM_Redemptions3;
--TD04
Select round(((sum(redeemers))/ (sum(selection))) *100,2) as Participation_rate
from TD04_2223_Fuel_DM_Redemptions3;
--TD05
Select round(((sum(redeemers))/ (sum(selection))) *100,2) as Participation_rate
from TD05_2223_Fuel_DM_Redemptions3;
--TD06
Select round(((sum(redeemers))/ (sum(selection))) *100,2) as Participation_rate
from TD06_2223_Fuel_DM_Redemptions3;

-- participation rate by segment
Select segment, round(((sum(redeemers))/ (sum(selection))) *100,2) as Participation_rate
from identifier($Fuel_DM_Redemptions3)
group by 1
Order by segment asc;
--TD08
Select segment,round(((sum(redeemers))/ (sum(selection))) *100,2) as Participation_rate
from TD08_2122_Fuel_DM_Redemptions3
group by 1
Order by segment asc;
--TD09
Select segment,round(((sum(redeemers))/ (sum(selection))) *100,2) as Participation_rate
from TD09_2122_Fuel_DM_Redemptions3
group by 1
Order by segment asc;
--TD10
Select segment, round(((sum(redeemers))/ (sum(selection))) *100,2) as Participation_rate
from TD10_2122_Fuel_DM_Redemptions3
group by 1
Order by segment asc;
--TD11
Select segment,round(((sum(redeemers))/ (sum(selection))) *100,2) as Participation_rate
from TD11_2122_Fuel_DM_Redemptions3
group by 1
Order by segment asc;
--TD01
Select segment,round(((sum(redeemers))/ (sum(selection))) *100,2) as Participation_rate
from TD01_2223_Fuel_DM_Redemptions3
group by 1
Order by segment asc;
--TD02
Select segment,round(((sum(redeemers))/ (sum(selection))) *100,2) as Participation_rate
from TD02_2223_Fuel_DM_Redemptions3
group by 1
Order by segment asc;
--TD03
Select segment,round(((sum(redeemers))/ (sum(selection))) *100,2) as Participation_rate
from TD03_2223_Fuel_DM_Redemptions3
group by 1
Order by segment asc;
--TD04
Select segment,round(((sum(redeemers))/ (sum(selection))) *100,2) as Participation_rate
from TD04_2223_Fuel_DM_Redemptions3
group by 1
Order by segment asc;
--TD05
Select segment,round(((sum(redeemers))/ (sum(selection))) *100,2) as Participation_rate
from TD05_2223_Fuel_DM_Redemptions3
group by 1
Order by segment asc;
--TD06
Select segment,round(((sum(redeemers))/ (sum(selection))) *100,2) as Participation_rate
from TD06_2223_Fuel_DM_Redemptions3
group by 1
Order by segment asc;


-- Total redemption rate
Select round(((sum(redemptions))/ (sum(selection))) *100,2) as redemption_rate
from identifier($Fuel_DM_Redemptions3);
--TD08
Select round(((sum(redemptions))/ (sum(selection))) *100,2) as redemption_rate
from TD08_2122_Fuel_DM_Redemptions3;
--TD09
Select round(((sum(redemptions))/ (sum(selection))) *100,2) as redemption_rate
from TD09_2122_Fuel_DM_Redemptions3;
--TD10
Select round(((sum(redemptions))/ (sum(selection))) *100,2) as redemption_rate
from TD10_2122_Fuel_DM_Redemptions3;
--TD11
Select round(((sum(redemptions))/ (sum(selection))) *100,2) as redemption_rate
from TD11_2122_Fuel_DM_Redemptions3;
--TD01
Select round(((sum(redemptions))/ (sum(selection))) *100,2) as redemption_rate
from TD01_2223_Fuel_DM_Redemptions3;
--TD02
Select round(((sum(redemptions))/ (sum(selection))) *100,2) as redemption_rate
from TD02_2223_Fuel_DM_Redemptions3;
--TD03
Select round(((sum(redemptions))/ (sum(selection))) *100,2) as redemption_rate
from TD03_2223_Fuel_DM_Redemptions3;
--TD04
Select round(((sum(redemptions))/ (sum(selection))) *100,2) as redemption_rate
from TD04_2223_Fuel_DM_Redemptions3;
--TD05
Select round(((sum(redemptions))/ (sum(selection))) *100,2) as redemption_rate
from TD05_2223_Fuel_DM_Redemptions3;
--TD06
Select round(((sum(redemptions))/ (sum(selection))) *100,2) as redemption_rate
from TD06_2223_Fuel_DM_Redemptions3;


-- Red rate by segment
Select segment, round(((sum(redeemers))/ (sum(selection))) *100,2) as Redemption_rate
from identifier($Fuel_DM_Redemptions2)
group by 1
Order by segment asc;
--TD08
Select segment, round(((sum(redemptions))/ (sum(selection))) *100,2) as Redemption_rate
from TD08_2122_Fuel_DM_Redemptions3
group by 1
Order by segment asc;
--TD09
Select segment, round(((sum(redemptions))/ (sum(selection))) *100,2) as Redemption_rate
from TD09_2122_Fuel_DM_Redemptions3
group by 1
Order by segment asc;
--TD10
Select segment, round(((sum(redemptions))/ (sum(selection))) *100,2) as Redemption_rate
from TD10_2122_Fuel_DM_Redemptions3
group by 1
Order by segment asc;
--TD11
Select segment, round(((sum(redemptions))/ (sum(selection))) *100,2) as Redemption_rate
from TD11_2122_Fuel_DM_Redemptions3
group by 1
Order by segment asc;
--TD01
Select segment, round(((sum(redemptions))/ (sum(selection))) *100,2) as Redemption_rate
from TD01_2223_Fuel_DM_Redemptions3
group by 1
Order by segment asc;

--TD02
Select segment, round(((sum(redemptions))/ (sum(selection))) *100,2) as Redemption_rate
from TD02_2223_Fuel_DM_Redemptions3
group by 1
Order by segment asc;
--TD03
Select segment, round(((sum(redemptions))/ (sum(selection))) *100,2) as Redemption_rate
from TD03_2223_Fuel_DM_Redemptions3
group by 1
Order by segment asc;
--TD04
Select segment, round(((sum(redemptions))/ (sum(selection))) *100,2) as Redemption_rate
from TD04_2223_Fuel_DM_Redemptions3
group by 1
Order by segment asc;
--TD05
Select segment, round(((sum(redemptions))/ (sum(selection))) *100,2) as Redemption_rate
from TD05_2223_Fuel_DM_Redemptions3
group by 1
Order by segment asc;
--TD06
Select segment, round(((sum(redeemers))/ (sum(selection))) *100,2) as Redemption_rate
from TD06_2223_Fuel_DM_Redemptions3
group by 1
Order by segment asc;


--
--
-- -- selection file vol
-- select segment, count(*) as volume from identifier($selectionfile) group by 1;
-- --TD08
-- select segment, count(*) as volume from TD08_2122_Petrol_DM group by 1;
-- --TD09
-- select segment, count(*) as volume from TD09_2122_Petrol_DM group by 1;
-- --TD10
-- select segment, count(*) as volume from TD10_2122_Petrol_DM group by 1;
-- --TD11
-- select segment, count(*) as volume from TD11_2122_Petrol_DM group by 1;
-- --TD01
-- select segment, count(*) as volume from TD01_2223_Petrol_DM  group by 1;
-- --TD02
-- select segment, count(*) as volume from TD02_2223_Petrol_DM group by 1;
-- --TD03
-- select segment, count(*) as volume from TD03_2223_Petrol_DM group by 1;
-- --TD04
-- select segment, count(*) as volume from TD04_2223_Petrol_DM group by 1;
-- --TD05
-- select segment, count(*) as volume from TD05_2223_Petrol_DM group by 1;
-- --TD06
-- select segment, count(*) as volume from TD06_2223_Petrol_DM group by 1;
--
-- select * from TD01_2223_Petrol_DM;
--
-- select count(*), split_test from TD01_2223_Petrol_DM where target_control_flag = 1 group by 2;
--

-- // TO CHECK IF EXCESS PRINTS ARE IN THE BACKUP FILE //
-- select substr(sr_id,4,11) from identifier($Prints4) where substr(sr_id,4,11) not in (select substr(sr_id,4,11) from identifier($Fuel_DM_Redemptions2));


-- // Union all tables together //
-- // Redemption //
create or replace table all_DM_redemptions as
select ec_id, redeem_qty, red_date, segment, threshold1,fin_week, campaign
from (select ec_id, redeem_qty, red_date, segment, threshold11 as threshold1,fin_week, '2122_TD08' as campaign from TD08_2122_Fuel_DM_Redemptions2
    union all select ec_id, redeem_qty, red_date, segment, threshold1 as threshold1,fin_week, '2122_TD09' as campaign from TD09_2122_Fuel_DM_Redemptions2
    union all select ec_id, redeem_qty, red_date, segment, threshold1 as threshold1,fin_week, '2122_TD10' as campaign from TD10_2122_Fuel_DM_Redemptions2
    union all select ec_id, redeem_qty, red_date, segment, threshold11 as threshold1,fin_week, '2122_TD11' as campaign from TD11_2122_Fuel_DM_Redemptions2
    union all select ec_id, redeem_qty, red_date, segment, threshold1 as threshold1,fin_week, '2223_TD01' as campaign from TD01_2223_Fuel_DM_Redemptions2
    union all select ec_id, redeem_qty, red_date, segment, threshold1 as threshold1,fin_week,'2223_TD02' as campaign from TD02_2223_Fuel_DM_Redemptions2
    union all select ec_id, redeem_qty, red_date, segment, threshold1 as threshold1,fin_week,'2223_TD03' as campaign from TD03_2223_Fuel_DM_Redemptions2
    union all select ec_id, redeem_qty, red_date, segment, threshold1 as threshold1,fin_week,'2223_TD04' as campaign from TD04_2223_Fuel_DM_Redemptions2
    union all select ec_id, redeem_qty, red_date, segment, threshold1 as threshold1,fin_week,'2223_TD05' as campaign from TD05_2223_Fuel_DM_Redemptions2
    union all select ec_id, redeem_qty, red_date, segment, threshold1 as threshold1,fin_week,'2223_TD06' as campaign from TD06_2223_Fuel_DM_Redemptions2);

select distinct ec_id, count(campaign) from all_DM_redemptions group by 1;
select * from all_DM_redemptions;

//CAT//
create or replace table all_CAT_redemptions as
select ec_id, redeem_qty, red_date, segment, threshold1,fin_week, campaign
from (select ec_id, redeem_qty, red_date, segment, threshold11 as threshold1,fin_week, '2122_TD08' as campaign from TD08_2122_CAT_Redemption4
    union all select ec_id, redeem_qty, red_date, segment, threshold1 as threshold1,fin_week, '2122_TD09' as campaign from TD09_2122_CAT_Redemption4
    union all select ec_id, redeem_qty, red_date, segment, threshold1 as threshold1,fin_week, '2122_TD10' as campaign from TD10_2122_CAT_Redemption4
    union all select ec_id, redeem_qty, red_date, segment, threshold11 as threshold1,fin_week, '2122_TD11' as campaign from TD11_2122_CAT_Redemption4
    union all select ec_id, redeem_qty, red_date, segment, threshold1 as threshold1,fin_week, '2223_TD01' as campaign from TD01_2223_CAT_Redemption4
    union all select ec_id, redeem_qty, red_date, segment, threshold1 as threshold1,fin_week,'2223_TD02' as campaign from TD02_2223_CAT_Redemption4
    union all select ec_id, redeem_qty, red_date, segment, threshold1 as threshold1,fin_week,'2223_TD03' as campaign from TD03_2223_CAT_Redemption4
    union all select ec_id, redeem_qty, red_date, segment, threshold1 as threshold1,fin_week,'2223_TD04' as campaign from TD04_2223_CAT_Redemption4
    union all select ec_id, redeem_qty, red_date, segment, threshold1 as threshold1,fin_week,'2223_TD05' as campaign from TD05_2223_CAT_Redemption4
    union all select ec_id, redeem_qty, red_date, segment, threshold1 as threshold1,fin_week,'2223_TD06' as campaign from TD06_2223_CAT_Redemption4);

select distinct ec_id, count(campaign) from all_CAT_redemptions group by 1;
select * from all_CAT_redemptions;

-- row per EC_ID on which campaign they redeemed in - can have more than one row if redeemed in more that one campaigns
create or replace table Campaigns_redeemed_in as
    select distinct ec_ID,
                    campaign,
                    1 as count_of_campaigns_redeemed
from all_CAT_redemptions
order by ec_id;

-- by EC_ID the number of campaigns they've redeemed in
create or replace table no_of_campaigns_redeemed as
    select ec_id,
           sum(count_of_campaigns_redeemed) as campaigns_redeemed
from Campaigns_redeemed_in
Group By 1;

-- no of people that have redeemed x no. of campaigns
create or replace table Campaigns_redeemed_distribution as
    select campaigns_redeemed,
           count(ec_id) as vol
from no_of_campaigns_redeemed
group by 1;







-- // Selections //
create or replace table all_selections as
select distinct ec_id, segment, threshold1, campaign, '1' as in_campaign
from (select distinct ec_id, segment, threshold11 as threshold1, '2122_TD08' as campaign  from TD08_2122_Petrol_DM
    union all select distinct ec_id, segment, threshold1 as threshold1, '2122_TD09' as campaign  from TD09_2122_Petrol_DM
    union all select  distinct ec_id, segment, threshold1 as threshold1, '2122_TD10' as campaign  from TD10_2122_Petrol_DM
    union all select distinct ec_id, segment, threshold11 as threshold1, '2122_TD11' as campaign from TD11_2122_Petrol_DM
    union all select distinct ec_id, segment, threshold1 as threshold1,'2223_TD01' as campaign from TD01_2223_Petrol_DM
    union all select distinct ec_id, segment, threshold1 as threshold1,'2223_TD02' as campaign from TD02_2223_Petrol_DM
    union all select  distinct ec_id, segment, threshold1 as threshold1,'2223_TD03' as campaign from TD03_2223_Petrol_DM
    union all select distinct ec_id, segment, threshold1 as threshold1,'2223_TD04' as campaign from TD04_2223_Petrol_DM
    union all select distinct ec_id, segment, threshold1 as threshold1,'2223_TD05' as campaign from TD05_2223_Petrol_DM
    union all select distinct ec_id, segment, threshold1 as threshold1,'2223_TD06' as campaign from TD06_2223_Petrol_DM);

select ec_id, count(campaign) from all_selections group by 1;

--grouped by EC_ID (one row per customer) how many selections they have been in i.e. how many packs/campaigns recieved
create or replace table count_selections1 as
select count(ec_id) as count_of_campaigns_received,
       ec_id
from all_selections
group by 2
order by ec_id;

-- get redemption table: --
-- no of people that have redeemed x no. of campaigns
create or replace table Campaigns_redeemed_in as
    select distinct ec_ID,
                    campaign,
                    1 as count_of_campaigns_redeemed
from all_CAT_redemptions
    order by ec_id;

--no of people that have redeemed x no. of campaigns grouped by ec_id
create or replace table no_of_campaigns_redeemed as
    select ec_id,
           sum(count_of_campaigns_redeemed) as campaigns_redeemed
from Campaigns_redeemed_in
Group By 1;

--join on the selection and redemption table
create or replace table distribution_by_customer as
    select a.ec_id, a.count_of_campaigns_received, zeroifnull(b.campaigns_redeemed) as campaign_redeemed
    from count_selections1 a
left join no_of_campaigns_redeemed b
on a.ec_id = b.ec_id
order by count_of_campaigns_received;

--gives the number of customers that were selected in x campaigns, how many campaigns they redeemed in
create or replace table final_distribution2 as
    select count_of_campaigns_received,
           campaign_redeemed,
           count(ec_id) as vol
from distribution_by_customer
    group by 1,2
order by count_of_campaigns_received;



--
-- select * from identifier ($selectionfile);
-- select * from identifier($Prints4);
-- select * from identifier($Fuel_DM_Redemptions2);
-- select * from identifier($CAT_Redemption4);
--
--
-- // PFS //



-- -- CAT redemptions at customer level
-- select ec_id,Count(*) Number_of_CAT_redemptions, segment, threshold1, fin_week, red_date
--       from identifier($CAT_Redemption4) group by 1,3,4,5,6;
--
--
-- -- CAT summary table (grouped by week, segment and threshold1)
-- select Count(distinct ec_id) as vol, segment, threshold1,fin_week
--       from identifier($CAT_Redemption4)
--       group by 2,3,4;
--
--
-- -- DM redemptions at customer level
-- select ec_id, sr_id, count (*),
--              sum(redeem_qty) as Number_of_DM_redemptions, fin_week, segment, threshold1
--              from identifier($Fuel_DM_Redemptions2)
--              group by 1,2,5,6,7;


select * from eDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_shopping_transaction_line where unit_of_measure = 'L' limit 50;
select * from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_payment_line order by payment_value desc limit 5; where transaction_number = '7393' and party_account_ID = '994491670220813' limit 5;


select count(ec_ID), count(distinct ec_ID) from TD08_2122_CAT_redemption4;

Select count(ec_id), count(distinct EC_ID) from fuel_litres;

--
select * from fuel_litres
order by ec_id desc;

create or replace temp table fuel_litres as
select distinct a.ec_id, b.fuel_litres,b.fuel_spend, a.payment_value,b.week_no, a.redeem_qty, a.red_date, b.party_account_id, row_number() over (partition by a.ec_ID order by a.red_date asc) as row_number
from TD09_2122_CAT_redemption4 a
         left join (select distinct
                        pa.enterprise_customer_id as ec_id,
                                    pa.party_account_id,
                        case when stl. party_account_type_code= '04' then 'Instore'
                             when stl.party_account_type_code = '02' then 'Online' end as channel,
                        dat.week_no,
                                    stl.transaction_date,
                        item_weight as fuel_litres,
                        extended_price as fuel_spend


                    from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_shopping_transaction_line as stl,
                         EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_c_location_map as lm,
                         EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_ean as ean,
                         EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_c_sku_map as sku,
                         EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_c_sub_category_map as scat,
                         EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_payment_line as pay,
                         EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_DATE_MAP as dat,
                         EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_PARTY_ACCOUNT as pa


                    where stl.location_key = lm.location_key
                      and lm.js_petrol_ind <> 'N'
                      and stl.ean_key = ean.ean_key
                      and ean.sku_key = sku.sku_key
                      and sku.sub_category_key = scat.sub_category_key
                      and stl.party_account_id=pay.party_account_id
                      and stl.party_account_type_code=pay.party_account_type_code
                      and stl.transaction_date=pay.transaction_date
                      and stl.transaction_time=pay.transaction_time
                      and stl.transaction_number=pay.transaction_number
                      and stl.location_key=pay.location_key
                      and stl.till_number=pay.till_number
                      and dat.calendar_date=stl.transaction_date
                      and pa.party_account_id=stl.party_account_id

--                       and dat.week_no between (select min(pre_period_start_fw) from pre_period_start) and $reporting_week

                        /*ONLY petrol category*/
                      and scat.sub_category = 839
                      and stl.unit_of_measure = 'L'
                    group by 1,2,3,4,5,6,7) as b
on a.ec_id = b.ec_id
and a.fin_week = b.week_no
and a.red_date = b.transaction_date;


select distinct
                        pa.enterprise_customer_id as ec_id,
                                    pa.party_account_id,
                        case when stl. party_account_type_code= '04' then 'Instore'
                             when stl.party_account_type_code = '02' then 'Online' end as channel,
                        dat.week_no,
                                    stl.transaction_date,
                        sum(stl.item_weight) as fuel_litres,
                        sum(stl.extended_price) as fuel_spend


                    from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_shopping_transaction_line as stl,
                         EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_c_location_map as lm,
                         EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_ean as ean,
                         EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_c_sku_map as sku,
                         EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_c_sub_category_map as scat,
                         EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_payment_line as pay,
                         EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_DATE_MAP as dat,
                         EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_PARTY_ACCOUNT as pa


                    where stl.location_key = lm.location_key
                      and lm.js_petrol_ind <> 'N'
                      and stl.ean_key = ean.ean_key
                      and ean.sku_key = sku.sku_key
                      and sku.sub_category_key = scat.sub_category_key
                      and stl.party_account_id=pay.party_account_id
                      and stl.party_account_type_code=pay.party_account_type_code
                      and stl.transaction_date=pay.transaction_date
                      and stl.transaction_time=pay.transaction_time
                      and stl.transaction_number=pay.transaction_number
                      and stl.location_key=pay.location_key
                      and stl.till_number=pay.till_number
                      and dat.calendar_date=stl.transaction_date
                      and pa.party_account_id=stl.party_account_id

--                       and dat.week_no between (select min(pre_period_start_fw) from pre_period_start) and $reporting_week

                        /*ONLY petrol category*/
                      and scat.sub_category = 839
                      and stl.unit_of_measure = 'L'
and ec_id = '50000023864761'
and stl.transaction_date = '2022-01-03'
                    group by 1,2,3,4,5,6,7




select * from TD09_2122_CAT_Redemption4 where  ec_id = '50000023864761';

select ec_id, fuel_litres, fuel_spend, payment_value, row_number
from fuel_litres a
where ec_id = '50000023864761'
Order by EC_ID asc;

select * from fuel_litres where ec_id = '50000023864761';

--50000016687704


--
-- create or replace temp table litre_red as
-- select a.ec_ID,
--        stl. item_weight,
--        row_number() over (partition by a.ec_ID order by a.red_date asc) as row_number,
--        a.redeem_qty
-- from TD08_2122_CAT_Redemption4 a
-- left join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_shopping_transaction_line as stl
-- on a.sr_ID = stl.Party_account_id
-- and a.red_date = stl.transaction_date
-- where stl.unit_of_measure = 'L';

create or replace temp table redeemed_once as
select ec_id,
       sum(redeem_qty) as sum_redeem_qty
       from TD09_2122_CAT_Redemption4
group by 1
having sum_redeem_qty =1;
-- 7,711


create or replace temp table redeemed_twice as
select ec_id,
       sum(redeem_qty) as sum_redeem_qty
       from TD09_2122_CAT_Redemption4
group by 1
having sum_redeem_qty =2;
-- 1,948

select avg(fuel_litres), row_number
from fuel_litres
where ec_id in (select ec_id from redeemed_once)
group by 2;


select * from fuel_litres
where ec_id in (select ec_id from redeemed_once) and row_number =2;

select * from fuel_litres where ec_id = '50000023864761';
select * from TD09_2122_CAT_Redemption4 where ec_id = '50000023864761';

select row_number, avg(fuel_litres)
from fuel_litres
where ec_id in (select ec_id from redeemed_once)
group by 1;


select row_number, avg(fuel_litres)
from fuel_litres
where ec_id in (select ec_id from redeemed_twice)
group by 1;



select row_number, avg(fuel_litres) from fuel_litres group by 1 order by row_number asc;






-- select count(a.ec_ID)
-- from TD08_2122_CAT_redemption4 a
--          left join (select distinct
--                         pa.enterprise_customer_id as ec_id,
--                         case when stl.party_account_type_code = '04' then 'Instore'
--                              when stl.party_account_type_code = '02' then 'Online' end as channel,
--                         dat.week_no,
--                         sum(stl.item_weight) as fuel_litres,
--                         sum(stl.extended_price) as fuel_spend
--
--                     from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_shopping_transaction_line as stl,
--                          EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_c_location_map as lm,
--                          EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_ean as ean,
--                          EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_c_sku_map as sku,
--                          EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_c_sub_category_map as scat,
--                          EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_payment_line as pay,
--                          EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_DATE_MAP as dat,
--                          EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_PARTY_ACCOUNT as pa
--
--
--                     where stl.location_key = lm.location_key
--                       and lm.js_petrol_ind <> 'N'
--                       and stl.ean_key = ean.ean_key
--                       and ean.sku_key = sku.sku_key
--                       and sku.sub_category_key = scat.sub_category_key
--                       and stl.party_account_id=pay.party_account_id
--                       and stl.party_account_type_code=pay.party_account_type_code
--                       and stl.transaction_date=pay.transaction_date
--                       and stl.transaction_time=pay.transaction_time
--                       and stl.transaction_number=pay.transaction_number
--                       and stl.location_key=pay.location_key
--                       and stl.till_number=pay.till_number
--                       and dat.calendar_date=stl.transaction_date
--                       and pa.party_account_id=stl.party_account_id
--
-- --                       and dat.week_no between (select min(pre_period_start_fw) from pre_period_start) and $reporting_week
--
--                         /*ONLY petrol category*/
--                       and scat.sub_category = 839
--                       and stl.unit_of_measure = 'L'
--                     group by 1,2,3) as b
-- on a.ec_id = b.ec_id
-- where a.fin_week = b.week_no;
--13564
--
-- select count(ec_ID) from td08_2122_CAT_redemption4
-- -- 13565

-- select ec_id
-- from td08_2122_CAT_redemption4
-- where ec_id not in (select ec_id from fuel_litres);
--
-- select * from td08_2122_CAT_redemption4
-- where ec_id = '50000098267301'




