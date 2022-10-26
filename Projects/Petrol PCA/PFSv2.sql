USE ROLE RL_PROD_MARKETING_ANALYTICS;
USE WAREHOUSE WHS_PROD_MARKETING_ANALYTICS_LARGE;
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA TD_REPORTING;



-- Creating Fuel extract in the last 12 weeks
create or replace temp table fuel_spend_extract as
select distinct sel.*,
                fl.channel,
                fl.week_no,
                fl.fuel_litres,
                fl.fuel_spend
from TD06_2223_Petrol_DM as sel
         inner join (
             -- Extracted from the Sales report
             select distinct
                        pa.enterprise_customer_id as ec_id,
                        case when stl.party_account_type_code = '04' then 'Instore'
                             when stl.party_account_type_code = '02' then 'Online' end as channel,
                        dat.week_no,
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
                      and dat.CALENDAR_DATE between dateadd(week, -12, '2022-08-20') and '2022-08-20'

                        /*ONLY petrol category*/
                      and scat.sub_category = 839
                      and stl.unit_of_measure = 'L'
                      and lm.location not in (2835,2341,2158,897,2271,2241,2240,2051,847,2304,2254,2301,2070,2400) -- remove pay at pump
                    group by 1,2,3
) as fl
                   on sel.ec_id = fl.ec_id

;
--shopped fuel (non pay at pump) in past 12 weeks
create or replace temp table fuel_spenders as
select ec_id, 'Yes' Fuel_Spender, Count(*) Number_Of_Fuel_Shops, Sum(fuel_litres) Total_Litres_Bought, Sum(fuel_spend) Total_Fuel_Spend, Avg(fuel_spend) Avg_Fuel_Spend
from fuel_spend_extract
Group by 1;

--
-- -- Create a table of Petrol Redeemers
-- Create or replace temp table Redeemers as
-- Select distinct Ec_ID, 'Redeemed' Redeemed
-- From TD06_2223_Redemptions
-- Where Campaign = 'TD06_2223_Petrol_DM';


-- Join Together

create or replace temp table Petrol_T06 as
Select
a.ec_id
, a.target_control_flag
, Case when b.PS_in_Fav_Store is not null then b.PS_in_Fav_Store else 'Fav store doesnt have a PFS' end as PFS_in_Main_Store
, case when Fuel_Spender = 'Yes' then 'Yes' else 'No' end as FuelSpender
, Number_Of_Fuel_Shops
     , Avg_Fuel_Spend
     , Total_Fuel_Spend
     , Case when d.redeem_qty = '1' then 'redeemed' else 'Did not' end as Redeemed

From TD06_2223_Petrol_DM a
Left join pref_store_petrol b on a.ec_id = b.ec_id
Left join fuel_spenders c on a.ec_id = c.ec_id
Left join TD06_2223_CAT_Redemption4 d on a.ec_id = d.ec_id;



--Question 1. Of those selected for Petrol TD06, how many have a Petrol Station in their favourite store
Select PFS_IN_MAIN_STORE, Count(*)
From Petrol_T06
Group by 1;


--Question 2. How many have bought Petrol in the last 12 weeks (since start of TD06)
Select
PFS_in_Main_Store
, FuelSpender
, Count(*) Volume
, zeroifnull(Avg(Number_Of_Fuel_Shops)) Avg_Fuel_Shops
, zeroifnull(Avg(Avg_Fuel_Spend)) Avg_Fuel_Spend_per_visit
From Petrol_T06
Where Target_Control_Flag = 1
Group by 1,2
Order by 1,2;


-- Question 3. Those that redeemed did they have a PFS in their fav store
Select
PFS_in_Main_Store
,Redeemed
, Count(*) Volume
From Petrol_T06
Where Target_Control_Flag = 1
Group by 1,2
Order by 1,2;

/*
-- join together overall
create or replace temp table ep_sg_m_pfs_flags as
select distinct a.enterprise_customer_id,
                a.plc,
                a.journey_instore,
                a.maintain_c,
                a.stretch_and_grow_c,
                zeroifnull(b.pfs_8km_flag) as pfs_8km_flag,
                zeroifnull(b.pfs_5km_flag) as pfs_5km_flag,
                zeroifnull(b.pfs_3km_flag) as pfs_3km_flag,
                zeroifnull(b.pfs_2km_flag) as pfs_2km_flag,
                zeroifnull(b.pfs_1km_flag) as pfs_1km_flag,
                iff(c.js_petrol_ind = 'Y', 1, 0) as pfs_preferred_store_flag,
                iff(d.enterprise_customer_id is not null, 1, 0) as fuel_spender_flag

from ep_sg_m as a
         left join ep_eg_close_to_pfs as b
                   on a.enterprise_customer_id = b.enterprise_customer_id
         left join pfs_fav_store2 as c
                   on a.enterprise_customer_id = c.enterprise_customer_id
         left join fuel_spenders as d
                   on a.enterprise_customer_id = d.enterprise_customer_id
;


select count(distinct enterprise_customer_id)
from ep_sg_m_pfs_flags
where journey_instore = 'Maintain' and plc = 0 and maintain_c = 0
and fuel_spender_flag = 1
;


select count(distinct enterprise_customer_id)
from ep_sg_m_pfs_flags
where journey_instore = 'Stretch & Grow' and plc = 0 and stretch_and_grow_c = 0
-- and pfs_preferred_store_flag = 1
;

select pfs_8km_flag, pfs_preferred_store_flag, fuel_spender_flag, count(distinct enterprise_customer_id)
from ep_sg_m_pfs_flags
where journey_instore = 'Maintain' and plc = 0 and maintain_c = 0
group by 1,2,3
;

select pfs_5km_flag, pfs_preferred_store_flag, fuel_spender_flag, count(distinct enterprise_customer_id)
from ep_sg_m_pfs_flags
where journey_instore = 'Maintain' and plc = 0 and maintain_c = 0
group by 1,2,3
;

select pfs_3km_flag, pfs_preferred_store_flag, fuel_spender_flag, count(*), count(distinct enterprise_customer_id)
from ep_sg_m_pfs_flags
where journey_instore = 'Maintain' and plc = 0 and maintain_c = 0
group by 1,2,3
;

select pfs_8km_flag, pfs_preferred_store_flag, fuel_spender_flag, count(distinct enterprise_customer_id)
from ep_sg_m_pfs_flags
where journey_instore = 'Stretch & Grow' and plc = 0 and stretch_and_grow_c = 0
group by 1,2,3
;

select pfs_5km_flag, pfs_preferred_store_flag, fuel_spender_flag, count(distinct enterprise_customer_id)
from ep_sg_m_pfs_flags
where journey_instore = 'Stretch & Grow' and plc = 0 and stretch_and_grow_c = 0
group by 1,2,3
;

select pfs_3km_flag, pfs_preferred_store_flag, fuel_spender_flag, count(*), count(distinct enterprise_customer_id)
from ep_sg_m_pfs_flags
where journey_instore = 'Stretch & Grow' and plc = 0 and stretch_and_grow_c = 0
group by 1,2,3
;





select count(*) from ep_sg_m_pfs_flags
where pfs_8km_flag = 1 or pfs_preferred_store_flag = 1 or fuel_spender_flag = 1
and plc = 0 and stretch_and_grow_c = 0
;

select count(*) from ep_sg_m_pfs_flags
where pfs_5km_flag = 1 or pfs_preferred_store_flag = 1 or fuel_spender_flag = 1
and plc = 0 and stretch_and_grow_c = 0
;
*/