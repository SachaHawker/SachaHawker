USE ROLE RL_PROD_MARKETING_ANALYTICS;
USE WAREHOUSE WHS_PROD_MARKETING_ANALYTICS_LARGE;
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA TD_REPORTING;


--
-- -- --TD08
-- Set campaignname = '2122_TD08';
-- SET campaign_start_date = '2021-10-31';
-- SET campaign_end_date = '2021-12-04';
-- SET rolling_date = '2021-12-18';
-- SET td ='TD08';
-- SET year ='2122';

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


--TD07
Set campaignname = '2223_TD07';
SET campaign_start_date = '2022-09-18';
SET campaign_end_date = '2022-10-29';
SET rolling_date = '2022-11-05';
SET td ='TD07';
SET year ='2223';


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
SET selectionfile = concat($td,'_',$year,'_Coupon_at_Till');
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
                 and campaign_type = 'Coupon_at_Till'
                 and (b.redeeming = 1 or b.printing = 1);



--creating barcode lookup, all DM barcodes for money off and fuel weeks for each Threshold
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
and a.flowchartname = 'Coupon_at_Till'
group by 1,2,3,4,5,6,7,8,9,9;



create or replace table identifier ($Prints1) as
select distinct pf.party_account_id as sr_id,
                pf.transaction_date,
                pf.transaction_time,
                case when left(cast(sr_id as varchar(15)), 1) = '4' then '04'
                     when left(cast(sr_id as varchar(15)), 1) = '2' then '02' else '99' end as party_account_type_code,
                pf.location,
                pf.print_qty,
                0 as redeem_qty,
                bl.campaignname,
                bl.reward_value_penceoff
from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_CATALINA_PRINT_FILE  as pf
         inner join identifier($Barcodes1) as bl
                    on substr(pf.barcode,1,7) = substr(bl.Barcode,1,7)
                    and pf.transaction_date between bl.offer_effective_date and bl.offer_expiration_date;


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

select * from identifier ($Prints4) where transaction_date = '2022-10-04';

select count(ec_ID), count(distinct EC_ID) from identifier ($Prints4) where transaction_date = '2022-10-05';




