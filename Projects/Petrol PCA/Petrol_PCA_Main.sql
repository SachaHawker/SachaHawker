USE ROLE RL_PROD_MARKETING_ANALYTICS;
USE WAREHOUSE WHS_PROD_MARKETING_ANALYTICS_LARGE;
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA TD_REPORTING;

SET firstdatecampaign = '2022-02-01';
Set campaignname = '2122_TD10';
SET petrol_cat_end_date = '2022-02-12';
SET campaign_start_date = '2022-01-02';
SET campaign_end_date = '2022-01-30';
SET td ='TD10';
SET year ='2122';
SET rolling_date = '2022-02-12';

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
               where a.week_start <= $firstdatecampaign
                 and a.week_end >= $firstdatecampaign
                 and campaign_type = 'Petrol_DM'
                 and (b.redeeming = 1 or b.printing = 1);

select * from TD10_Barcodes1;

--creating barcode lookup, all DM barcodes for money off and fuel weeks for each threshold
create or replace table identifier ($Barcodes1) as
Select
a.flowchartname,
a.campaignname,
a.flowchartid,
c.barcode,
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
where a.campaignname = $campaignname
and a.flowchartname = 'Petrol_DM'
group by 1,2,3,4,5,6;


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
-- 15,169 - This is all redeemers, including mal redeemers


-- Redeemers just in that campaign
create or replace table identifier ($CAT_Redemption3) as
select * from identifier ($CAT_Redemption2)
where ec_id in (select ec_id from IDENTIFIER($SELECTIONFILE) where target_control_flag = 1);
Select count(*) from identifier ($CAT_Redemption3);
-- 9,044 - Redeemers in total (may have some from other campaigns- reuse barcodes)

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
                bl.campaignname
from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_CATALINA_PRINT_FILE  as pf
         inner join identifier($Barcodes2) as bl
                    on substr(pf.barcode,1,7) = substr(bl.petrol_Barcode,7,7)
                    and pf.transaction_date between bl.campaign_start_date and bl.campaign_end_date;]

-- add on EC_ID
create or replace table identifier ($Prints2) as
    select  b.enterprise_customer_id as ec_id, a.*
from identifier ($Prints1) as a
left join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_PARTY_ACCOUNT as b
on a.sr_id = b.party_account_id;

select count(*) from identifier ($Prints2);
-- total prints 10,229 - this includes mal redeemers


-- only prints from people that are in selection file
Create or replace table identifier ($Prints3) as
    Select * from identifier ($Prints2)
where EC_ID in (select ec_id from IDENTIFIER($SELECTIONFILE) where target_control_flag = 1);
Select * from identifier ($Prints3);
--7,053 - prints from only those in selection file

create or replace table identifier ($Prints4) as
    select a.*,
           b.fin_week,
           b.week_start,
           b.week_end,
           c.threshold,
           c.segment
    from identifier ($Prints3) as a
left join td_date_map as b
    left join IDENTIFIER($SELECTIONFILE) c
    on a.ec_id = c.ec_id
where a.transaction_date between b.week_start and b.week_end;


-- Redeemers where redemption is after print date, excluding those from other campaigns
create or replace table identifier($CAT_Redemption4) as
select distinct b.ec_id,
       b.redeem_qty,
       b.red_date,
       case when b.red_date < a.print_date then 1 else 0 end as unrelated_redemption_flag,
       c.threshold,
       c.segment,
       b.Payment_value,
       b.fin_week,
       b.transaction_value
from (select ec_id,
             sum(print_qty) as print_qty,
             min(transaction_date) as print_date,
             fin_week
from identifier($Prints4) group by 1,4) as a
left join (select ec_id,
                  redeem_qty as redeem_qty,
                  transaction_date as red_date,
                  payment_value as Payment_value,
                  fin_week,
                  transaction_value
from identifier($CAT_Redemption3)) as b on a.ec_id = b.ec_id
Left join IDENTIFIER($SELECTIONFILE) as c
on b.ec_id = c.ec_id
where unrelated_redemption_flag = 0 and redeem_qty is not null;



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
-- 29,176 - total redeeemers in this campaign (petrol and money off)

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
       b.Threshold,
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
From identifier($DM_Redemptions3) as a
Left join identifier($selectionfile) as b
on a.ec_id = b.ec_id
where offer_cell  like 'SPET%'
Group by 1,2,3,5,6,7,8,9,10,11;

-- transactional level
create or replace table identifier($Fuel_DM_Redemptions1) as
select  b.ec_id,
       b.redeem_qty,
       b.red_date,
       c.threshold,
       c.segment,
       b.transaction_value,
       b.fin_week,
b. sr_id
from identifier($Fuel_DM_Redemptions) as b
Left join IDENTIFIER($SELECTIONFILE) as c
on b.ec_id = c.ec_id
select * from identifier($Fuel_DM_Redemptions1);
-- 6,892

select distinct ec_id, sum(redeem_qty) as vol from identifier($Fuel_DM_Redemptions1) group by 1 having vol > 1 ;
-- 895 with more than 1 Dm redemption

-- customer level
create or replace table identifier($Fuel_DM_Redemptions2) as
    select distinct ec_id,
           threshold,
           segment,
           sum(redeem_qty) as redeem_qty,
           sr_id,
           fin_week,
           red_date,
           transaction_value
from identifier($Fuel_DM_Redemptions1)
group by 1,2,3,5,6,7,8;




-- CAT redemptions at customer level
select ec_id,Count(*) Number_of_CAT_redemptions, segment, threshold, fin_week, red_date
      from identifier($CAT_Redemption4) group by 1,3,4,5,6;




-- CAT summary table (grouped by week, segment and threshold)
select Count(distinct ec_id) as vol, segment, threshold,fin_week
      from identifier($CAT_Redemption4)
      group by 2,3,4;



-- DM redemptions at customer level
select ec_id, sr_id, count (*),
             sum(redeem_qty) as Number_of_DM_redemptions, fin_week, segment, threshold
             from identifier($Fuel_DM_Redemptions2)
             group by 1,2,5,6,7;

-- DM summary
select Count(distinct ec_id) as vol, fin_week, segment, threshold
from identifier($Fuel_DM_Redemptions2)
group by 2,3,4;
--6884


-- Prints customer level
Select ec_id, sr_id, Count(*) number_of_prints, fin_week, segment, threshold
      From identifier($Prints4)
    Group by 1,2,4,5,6;

-- prints summary
select Count(distinct ec_id) as vol, fin_week, threshold, segment
from identifier($Prints4)
group by 2,3,4;
--6961



// FINAL REDEMPTION TABLES //

select * from identifier($Prints4);
select * from identifier($Fuel_DM_Redemptions2);
select * from identifier($CAT_Redemption4);


//QUESTIONS//

// Target Vol //
select segment, count(*) as volume from identifier($selectionfile) group by 1;
select threshold, count(*) as volume from identifier($selectionfile) group by 1 order by threshold asc;

// CAT redemptions //

-- total no. of CAT redemptions
select count (*) as vol from identifier($CAT_Redemption4);
-- 5494

-- no. of CAT redemptions by number
Select distinct count(ec_id) as volume, Number_of_CAT_redemptions
      From (select ec_id, count(*) as Number_of_CAT_redemptions
      from identifier($CAT_Redemption4) group by 1)
group by 2;

-- no. of CAT redemptions per redeemer
Select distinct ec_id, Number_of_CAT_redemptions
      From (select ec_id, count(*) as Number_of_CAT_redemptions
      from identifier($CAT_Redemption4) group by 1)
group by 1,2;
-- 4595 redeemers




// DM redemptions //

-- total no. of DM redemptions
select count (redeem_qty) as vol from identifier($Fuel_DM_Redemptions2);
--6884

-- no. of DM redemptions by number
Select distinct count(ec_id) as volume, Number_of_DM_redemptions
      From (select ec_id, count(redeem_qty) as Number_of_DM_redemptions
      from identifier($Fuel_DM_Redemptions2) group by 1)
    Group by 2;



// PRINTS //

-- NOTE: all excess prints are accounted for in the back up file

-- Total no. of prints
select count (*) as vol from identifier($Prints4);
-- 7051

-- no. of prints by number
Select distinct count(ec_id) as volume, number_of_prints
      From (select ec_id, count(*) as number_of_prints
      from identifier($Prints4)
      group by 1)
    Group by 2;


 //Engagement by week (CAT Redemptions)
 select count (*) as volume, fin_week from identifier($CAT_Redemption4) group by 2;
 select count (*) as volume, fin_week from identifier($Fuel_DM_Redemptions2) group by 2;
 select count (*) as volume, fin_week from identifier($Prints4) group by 2;


// Engagement by day //
select count(*) as volume,red_date from identifier($Fuel_DM_Redemptions2) group by 2;
select count(*) as volume, red_date from identifier($CAT_Redemption4) group by 2;

// how much do customers spend on fuel //
select avg(transaction_value) as average_spend, segment from identifier($CAT_Redemption4) group by 2;
select avg(transaction_value) as average_spend, threshold from identifier($CAT_Redemption4) group by 2;

// How much do customers spend in shop to meet threshold//
select avg(transaction_value) as average_spend, segment from identifier($Fuel_DM_Redemptions2) group by 2;
select threshold, avg(transaction_value) as average_spend  from identifier($Fuel_DM_Redemptions2) group by 1 order by threshold ASC;


// Red rate //





-- selection volume / redemptions
Select segment, sum((select segment, count(*) as volume from identifier($selectionfile) group by 1) /
                    (Select distinct count(ec_id) as volume,segment
                    From (select ec_id, segment --, count(*) as Number_of_CAT_redemptions
                    from identifier($CAT_Redemption4) group by 1,2)
                    Group by 2))
    from identifier($CAT_Redemption4) as red_rate
group by 1;


-- no. of CAT redemptions by number
create or replace temp table CAT_redemptions as
Select distinct count(ec_id) as volume,segment
      From (select ec_id, segment, count(*) as Number_of_CAT_redemptions
      from identifier($CAT_Redemption4) group by 1,2)
    Group by 2;

-- selection file vol
select segment, count(*) as volume from identifier($selectionfile) group by 1;

select * from identifier($CAT_Redemption4);


// TO CHECK IF EXCESS PRINTS ARE IN THE BACKUP FILE //
select substr(sr_id,4,11) from identifier($Prints4) where substr(sr_id,4,11) not in (select substr(sr_id,4,11) from identifier($Fuel_DM_Redemptions2));

// Union all tables together //
// Redemption //
create or replace table all_redemptions as
select ec_id, redeem_qty, red_date, segment, threshold,fin_week, campaign
from (select ec_id, redeem_qty, red_date, segment, threshold,fin_week, '2122_TD10' as campaign from TD10_2122_CAT_Redemption4
    union all select ec_id, redeem_qty, red_date, segment, threshold,fin_week, '2122_TD11' as campaign from TD11_2122_CAT_Redemption4
    union all select ec_id, redeem_qty, red_date, segment, threshold,fin_week, '2223_TD01' as campaign from TD01_2223_CAT_Redemption4
    union all select ec_id, redeem_qty, red_date, segment, threshold,fin_week,'2223_TD02' as campaign from TD02_2223_CAT_Redemption4
    union all select ec_id, redeem_qty, red_date, segment, threshold,fin_week,'2223_TD03' as campaign from TD03_2223_CAT_Redemption4
    union all select ec_id, redeem_qty, red_date, segment, threshold,fin_week,'2223_TD04' as campaign from TD04_2223_CAT_Redemption4
    union all select ec_id, redeem_qty, red_date, segment, threshold,fin_week,'2223_TD05' as campaign from TD05_2223_CAT_Redemption4);

select ec_id, count(campaign) from all_redemptions group by 1;

// Selections //
create or replace table all_selections as
select ec_id, segment, threshold, campaign, '1' as in_campaign
from (select ec_id, segment, threshold, '2122_TD10' as campaign  from TD10_2122_Petrol_DM
    union all select ec_id, segment, threshold1, '2122_TD11' as campaign from TD11_2122_Petrol_DM
    union all select ec_id, segment, threshold,'2223_TD01' as campaign from TD01_2223_Petrol_DM
    union all select ec_id, segment, threshold,'2223_TD02' as campaign from TD02_2223_Petrol_DM
    union all select ec_id, segment, threshold,'2223_TD03' as campaign from TD03_2223_Petrol_DM
    union all select ec_id, segment, threshold,'2223_TD04' as campaign from TD04_2223_Petrol_DM
    union all select ec_id, segment, threshold,'2223_TD05' as campaign from TD05_2223_Petrol_DM);

select ec_id, count(campaign) from all_selections group by 1;

select * from identifier ($selectionfile);
select * from identifier($Prints4);
select * from identifier($Fuel_DM_Redemptions2);
select * from identifier($CAT_Redemption4);


// PFS //