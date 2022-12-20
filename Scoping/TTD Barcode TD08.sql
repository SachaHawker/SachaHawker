USE ROLE RL_PROD_MARKETING_ANALYTICS;
USE WAREHOUSE WHS_PROD_MARKETING_ANALYTICS_LARGE;
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA td_REPORTING;

// Create table of those that were targetted//
create or replace temp table TD08_Standard_DM_Target as
    select * from TD08_2223_Standard_DM
where target_control_flag = 1;


-- Create my own date and campaign map, normal one is live, this is just for this campaign
create or replace temp table TD_Date_And_Campaign_Map_SH_TD08 as
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
               where a.week_start <= '2022-10-30'
                 and a.week_end >= '2022-10-30'
                 and campaign_type = 'Standard_DM'
                 and (b.redeeming = 1 or b.printing = 1);


--creating barcode lookup
create or replace temp table Barcodes_SH as
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
flowchartname from TD_Date_And_Campaign_Map_SH_TD08 ) as td
on a.flowchartname = td.flowchartname
and a.campaignname = td.trade_driver
where a.campaignname = '2223_TD08'
and a.flowchartname = 'Standard_DM'
group by 1,2,3,4,5,6,7,8,9;

-- add start, end and rolling date
Create or replace temp table barcodes2_SH as
Select
*,
'2022-10-30' as campaign_start_date,
'2022-12-03' as campaign_end_date,
'14' rolling_days_validity
from Barcodes_SH;

-- Find the points redemptions for those that redeemed TTD coupon
create or replace temp table points_redemptions as
select distinct pa.enterprise_customer_id as ec_id,
                point_red.party_account_id as sr_id,
                point_red.transaction_date,
                to_time(right(cast(1000000 + point_red.transaction_time as varchar(7)), 6)) as transaction_time,
                point_red.transaction_number,
                st.party_account_type_code,
                l.location,
                st.transaction_value,
                bl.barcode,
                point_red.points_earned*0.00425 as payment_value,
                0 as print_qty,
                1 as redeem_qty,
                0 as online_flag,
                0 as pfs_red
from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_loyalty_coupon_redemption as point_red
         inner join (select * from barcodes2_SH) as bl
                    on substring(bl.barcode ,8 ,7) = point_red.coupon_id
                        and length(bl.barcode)=15
                        or (substring(bl.barcode, 6, 7) = point_red.coupon_id
                        and point_red.transaction_date between '2022-10-30' and '2022-12-03')

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
                   on pa.party_account_id        = st.party_account_id
where barcode = '280025022069990';



// Redemptions of those that were selected//
create or replace temp table TD08_standard_DM_Redemptions as
    select a.*,  b.full_nectar_card_num from points_redemptions a
inner join TD08_Standard_DM_Target b
on a.ec_id = b.ec_id;



-- PETROL
-- Create my own date and campaign map, normal one is live, this is just for this campaign
create or replace temp table TD_Date_And_Campaign_Map_SH_TD08_pet as
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
               where a.week_start <= '2022-10-30'
                 and a.week_end >= '2022-10-30'
                 and campaign_type = 'Petrol_DM'
                 and (b.redeeming = 1 or b.printing = 1);


--creating barcode lookup, all DM barcodes for money off and fuel weeks for each threshold1
create or replace temp table Barcodes_SH_pet as
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
flowchartname from TD_Date_And_Campaign_Map_SH_TD08_Pet ) as td
on a.flowchartname = td.flowchartname
and a.campaignname = td.trade_driver
where a.campaignname = '2223_TD08'
and a.flowchartname = 'Petrol_DM'
group by 1,2,3,4,5,6,7,8,9;


-- add start, end and rolling date
Create or replace temp table barcodes2_SH_Pet as
Select
*,
'2022-10-30' as campaign_start_date,
'2022-12-03' as campaign_end_date,
'14' rolling_days_validity
from Barcodes_SH_pet;


-- Create points redemptions table of those that redeemed the TTD coupon
create or replace temp table points_redemptions_pet as
select distinct pa.enterprise_customer_id as ec_id,
                point_red.party_account_id as sr_id,
                point_red.transaction_date,
                to_time(right(cast(1000000 + point_red.transaction_time as varchar(7)), 6)) as transaction_time,
                point_red.transaction_number,
                st.party_account_type_code,
                l.location,
                st.transaction_value,
                bl.barcode,
                point_red.points_earned*0.00425 as payment_value,
                0 as print_qty,
                1 as redeem_qty,
                0 as online_flag,
                0 as pfs_red
from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_loyalty_coupon_redemption as point_red
         inner join (select * from barcodes2_SH_pet) as bl
                    on substring(bl.barcode ,8 ,7) = point_red.coupon_id
                        and length(bl.barcode)=15
                        or (substring(bl.barcode, 6, 7) = point_red.coupon_id
                        and point_red.transaction_date between '2022-10-30' and '2022-12-03')

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
                   on pa.party_account_id        = st.party_account_id
where barcode = '280025022070002';

-- Create target customers for petrol
create or replace temp table TD08_Petrol_DM_Target as
Select * from td08_2223_petrol_dm where target_control_flag =1;

-- only get redemptions from those selected
create or replace temp table td08_Petrol_DM_redemptions as
    select a.*, b.full_nectar_card_num from points_redemptions_pet a
inner join TD08_Petrol_DM_Target b
on a.ec_id = b.ec_id;


-- join on petrol and standard redemptions
create or replace temp table total_DM_redemptions as
    select distinct full_nectar_card_num, ec_id
    from td08_Petrol_DM_redemptions
union all
select distinct full_nectar_card_num, ec_id from TD08_standard_DM_Redemptions;

select count(distinct full_nectar_card_num), count(full_nectar_card_num) from total_DM_redemptions;

--points for account and formatting for beb file
create or replace temp table beb_amounts as
select distinct ec_id,
                substring(full_nectar_card_num,len(full_nectar_card_num)-10,len(full_nectar_card_num))  as party_account_no,
                full_nectar_card_num as rewardcardnumber,
                250 as points_reward,
                points_reward*0.00425 as points_cost,
                current_date() as beb_date
from total_DM_redemptions
;

set transdate = (to_char(dateadd(days, -1, current_timestamp), 'DD/MM/YYYY HH:SS:MI'));

--add points
CREATE OR REPLACE temp TABLE Beb_File AS
SELECT       98263000   AS IIN,
       SUBSTR(REWARDCARDNUMBER,9,11) AS CARDNUMBER,
       9106       AS STORENUMBER,
       99         AS OUTLETTYPE,
       999        AS REASONCODE,
       'SSLBONUS' AS OFFERCODE,
       $TRANSDATE AS TRANSDATE,
       0          AS TRANSVALUE,
       0          AS BASEPOINTS,
       points_reward AS BONUSPOINTS,
       0          AS PROMOPOINTS,
       0          AS POINTSEXCHANGED,
       0          AS VOUCHERSISSUES,
       'TDAnalyst'    AS CASHIERID
FROM beb_amounts
;

select * from Beb_File;

select $transdate;






