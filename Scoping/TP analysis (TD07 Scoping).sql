USE ROLE RL_PROD_MARKETING_ANALYTICS;
USE WAREHOUSE WHS_PROD_MARKETING_ANALYTICS_LARGE;
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA TD_REPORTING;

//TD04//
-- Make my own campaign map
create or replace temp table td_date_and_campaign_map_TD04 as
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
                 and a.week_end >= '2022-07-17'
                 and campaign_type = 'Triple_Points';

select * from td_date_and_campaign_map_TD04;

-- create barcode
create or replace temp table TP_TD04_barcode as
Select distinct
    c.REWARD_VALUE_POINTMULTI,
    td.campaign,
                c.barcode
from EDWS_PROD.PROD_CMT_PRESENTATION.vw_chrh_campaign_ec as a
inner join EDWS_PROD.PROD_CMT_PRESENTATION.vw_ch_fact_ec b
on a.flowchartid = b.flowchartid
inner join EDWS_PROD.PROD_CMT_PRESENTATION.vw_ch_offer_attribute_ec as c
on b.treatmentcode = c.treatmentcode
left join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_PARTY_ACCOUNT as ec
on ec.PARTY_ACCOUNT_ID = b.SR_ID
left join td_date_and_campaign_map_TD04 as td
on a.flowchartname = td.flowchartname
and a.campaignname = td.trade_driver
inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_shopping_transaction as d
on ec.PARTY_ACCOUNT_ID = d.party_account_ID
where a.campaignname = '2223_TD04'
and a.flowchartname = 'Triple_Points'
and td.trade_driver = '2223_TD04'


Select * from TP_TD04_barcode;





create or replace temp table instore_points_TD04 as
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
                bl.campaign,
                0 as online_flag,
                0 as pfs_red
from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_loyalty_coupon_redemption as point_red
         inner join (select * from TP_TD04_barcode) as bl
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
                   on pa.party_account_id        = st.party_account_id
;

select * from instore_points_TD04

select count(distinct ec_id) as redeemers,
       avg(transaction_value) as avg_spend,
       min(transaction_value) as min_spend,
       max(transaction_value) as max_spend,
       avg(payment_value) as avg_points_cost,
       avg(payment_value/0.00425) as avg_points,
       sum((payment_value)/0.00425) as total_points,
       sum(payment_value) as total_points_cost,
       sum(payment_value)/count(distinct ec_id) as points_cost_PC,
       sum(payment_value/0.00425)/count(distinct ec_id) as points_PC
from instore_points_TD04;

// TD02 //
-- Make my own campaign map
create or replace temp table td_date_and_campaign_map_TD02 as
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
               where a.week_start <= '2022-05-15'
                 and a.week_end >= '2022-05-15'
                 and campaign_type = 'Triple_Points';


-- create barcode
create or replace temp table TP_TD02_barcode as
Select distinct
    c.REWARD_VALUE_POINTMULTI,
    td.campaign,
                c.barcode
from EDWS_PROD.PROD_CMT_PRESENTATION.vw_chrh_campaign_ec as a
inner join EDWS_PROD.PROD_CMT_PRESENTATION.vw_ch_fact_ec b
on a.flowchartid = b.flowchartid
inner join EDWS_PROD.PROD_CMT_PRESENTATION.vw_ch_offer_attribute_ec as c
on b.treatmentcode = c.treatmentcode
left join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_PARTY_ACCOUNT as ec
on ec.PARTY_ACCOUNT_ID = b.SR_ID
left join td_date_and_campaign_map_TD02 as td
on a.flowchartname = td.flowchartname
and a.campaignname = td.trade_driver
inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_shopping_transaction as d
on ec.PARTY_ACCOUNT_ID = d.party_account_ID
where a.campaignname = '2223_TD02'
and a.flowchartname = 'Triple_Points'
and td.trade_driver = '2223_TD02'


Select * from TP_TD02_barcode;

create or replace temp table instore_points_TD02 as
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
                bl.campaign,
                0 as online_flag,
                0 as pfs_red
from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_loyalty_coupon_redemption as point_red
         inner join (select * from TD_REPORTING.TP_TD02_barcode) as bl
                    on substring(bl.barcode ,8 ,7) = point_red.coupon_id
                        and length(bl.barcode)=15
                        or (substring(bl.barcode, 6, 7) = point_red.coupon_id
                        and point_red.transaction_date between '2022-05-15' and '2022-06-18')

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
;

select * from instore_points_TD02

select count(distinct ec_id) as redeemers,
       avg(transaction_value) as avg_spend,
       min(transaction_value) as min_spend,
       max(transaction_value) as max_spend,
       avg(payment_value) as avg_points_cost,
       avg(payment_value/0.00425) as avg_points,
       sum((payment_value)/0.00425) as total_points,
       sum(payment_value) as total_points_cost,
       sum(payment_value)/count(distinct ec_id) as points_cost_PC,
       sum(payment_value/0.00425)/count(distinct ec_id) as points_PC
from instore_points_TD02




// TD01 //


-- Make my own campaign map
create or replace temp table td_date_and_campaign_map_TD01 as
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
               where a.week_start <= '2022-03-13'
                 and a.week_end >= '2022-03-13'
                 and campaign_type = 'Triple_Points';


-- create barcode
create or replace temp table TP_TD01_barcode as
Select distinct
    c.REWARD_VALUE_POINTMULTI,
    td.campaign,
                c.barcode
from EDWS_PROD.PROD_CMT_PRESENTATION.vw_chrh_campaign_ec as a
inner join EDWS_PROD.PROD_CMT_PRESENTATION.vw_ch_fact_ec b
on a.flowchartid = b.flowchartid
inner join EDWS_PROD.PROD_CMT_PRESENTATION.vw_ch_offer_attribute_ec as c
on b.treatmentcode = c.treatmentcode
left join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_PARTY_ACCOUNT as ec
on ec.PARTY_ACCOUNT_ID = b.SR_ID
left join td_date_and_campaign_map_TD01 as td
on a.flowchartname = td.flowchartname
and a.campaignname = td.trade_driver
inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_shopping_transaction as d
on ec.PARTY_ACCOUNT_ID = d.party_account_ID
where a.campaignname = '2223_TD01'
and a.flowchartname = 'Triple_Points'
and td.trade_driver = '2223_TD01'


Select * from TP_TD01_barcode;

create or replace temp table instore_points_TD01 as
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
                bl.campaign,
                0 as online_flag,
                0 as pfs_red
from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_loyalty_coupon_redemption as point_red
         inner join (select * from TD_REPORTING.TP_TD01_barcode) as bl
                    on substring(bl.barcode ,8 ,7) = point_red.coupon_id
                        and length(bl.barcode)=15
                        or (substring(bl.barcode, 6, 7) = point_red.coupon_id
                        and point_red.transaction_date between '2022-03-13' and '2022-04-16')

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
;

select * from instore_points_TD01

select count(distinct ec_id) as redeemers,
       avg(transaction_value) as avg_spend,
       min(transaction_value) as min_spend,
       max(transaction_value) as max_spend,
       avg(payment_value) as avg_points_cost,
       avg(payment_value/0.00425) as avg_points,
       sum((payment_value)/0.00425) as total_points,
       sum(payment_value) as total_points_cost,
       sum(payment_value)/count(distinct ec_id) as points_cost_PC,
       sum(payment_value/0.00425)/count(distinct ec_id) as points_PC
from instore_points_TD01



-- select * from EDWS_PROD.PROD_CMT_PRESENTATION.vw_chrh_campaign_ec limit 5;
-- select * from EDWS_PROD.PROD_CMT_PRESENTATION.vw_ch_fact_ec limit 100;
-- select * from EDWS_PROD.PROD_CMT_PRESENTATION.vw_ch_offer_attribute_ec limit 5;
-- select * from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_PARTY_ACCOUNT limit 5;
-- select * from td_date_and_campaign_map limit 5;
-- select * from td_date_and_campaign_map_TD04;
-- select * from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_shopping_transaction limit 5;
-- select * from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_payment_line limit 5;


