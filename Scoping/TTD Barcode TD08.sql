USE ROLE RL_PROD_MARKETING_ANALYTICS;
USE WAREHOUSE WHS_PROD_MARKETING_ANALYTICS_LARGE;
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA td_REPORTING;

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

select * from EDWS_PROD.PROD_CMT_PRESENTATION.vw_chrh_campaign_ec where flowchartname = 'Standard_DM'  limit 5;

--creating barcode lookup, all DM barcodes for money off and fuel weeks for each threshold1
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

Select * from barcodes_SH where barcode = '280025022069990';


-- add start, end and rolling date
Create or replace temp table barcodes2_SH as
Select
*,
'2022-10-30' as campaign_start_date,
'2022-12-03' as campaign_end_date,
'14' rolling_days_validity
from Barcodes_SH;


create or replace temp table Redemptions_TD08 as
select distinct
    pa.enterprise_customer_id as ec_id,
    pl.party_account_id as sr_id,
    pl.transaction_date,
    to_time(right(cast(1000000 + pl.transaction_time as varchar(7)), 6)) as transaction_time,
    pl.transaction_number,
    st.party_account_type_code,
    bl.barcode,
    l.location,
    st.transaction_value,
    bl.campaign_start_date,
    0 as print_qty,
    1 as redeem_qty,
    bl.campaignname,
    bl.reward_value_penceoff,
    '' as offer_code,
    null as threshold1,
    0 as online_flag,
    1 as pfs_red,
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
                                    campaign_start_date,
                                    campaign_end_date,
                            reward_value_penceoff,
                            Barcode
            from barcodes2_SH) as bl
    on pl.coupon_id = bl.barcode
        and pl.transaction_date between to_date(bl.campaign_start_date) and to_date(bl.campaign_end_date)

where pl.payment_type_code = '003'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,16,17,18;

Select count(*),transaction_date from Redemptions_TD08 group by transaction_date;



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
;

select * from points_redemptions where barcode = '280025022069990';

select count (distinct ec_id) from  points_redemptions where barcode = '280025022069990';


create or replace temp table redemptions2_TD08 as
select a.*, b.fin_week, b.week_start, b.week_end
from points_redemptions as a
left join TD_date_map as b
where a.transaction_date between b.week_start and b.week_end;

select count (*), fin_week from  redemptions2_TD08 where barcode = '280025022069990' group by 2;












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
;

select * from points_redemptions where barcode = '280025022069990';

select count (distinct ec_id) from  points_redemptions where barcode = '280025022069990';


create or replace temp table redemptions2_TD08_pet as
select a.*, b.fin_week, b.week_start, b.week_end
from points_redemptions_pet as a
left join TD_date_map as b
where a.transaction_date between b.week_start and b.week_end;

select count (*), fin_week from  redemptions2_TD08_pet where barcode = '280025022070002' group by 2;



Select count (*) from td08_2223_petrol_dm where target_control_flag =1;