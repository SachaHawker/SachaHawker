USE ROLE RL_PROD_MARKETING_ANALYTICS;
USE WAREHOUSE WHS_PROD_MARKETING_ANALYTICS_LARGE;
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA TD_REPORTING;


-- base pot (redeemers)
create or replace temp table CAT_redeemers as
Select distinct ec_id, Number_of_CAT_redemptions
      From (select ec_id, count(*) as Number_of_CAT_redemptions
      from identifier($CAT_Redemption4) group by 1)
group by 1,2;



-- what volume of stretch & grow live within 8km of a PFS

create or replace temp table ep_eg_close_to_pfs as
    select distinct a.*,
                    b.pfs_8km_flag,
                    iff(b.distance<5, 1, 0) as pfs_5km_flag,
                    iff(b.distance<3, 1, 0) as pfs_3km_flag,
                    iff(b.distance<2, 1, 0) as pfs_2km_flag,
                    iff(b.distance<1, 1, 0) as pfs_1km_flag,
                    b.store_name,
                    b.postcode_store,
                    b.postcode,
                    b.distance
from ep_sg_m as a
inner join customer_pfs_closeness_flag as b
on a.enterprise_customer_id = b.ec_id
where b.pfs_8km_flag = 1
and b.store_cd not in (46838,20918,798,12769,17764,24062,24553,43311,937,19085,834,969,19836,2795) -- remove pay at pump
;



-- what volume of stretch & grow have pfs home store

create or replace temp table pfs_fav_store as
    select distinct *, 'Y' as js_petrol_ind
    from -- 1. get most recent preferred store from customer_dim
         (select a.*,
                 cvu.PREFERRED_STORE,
                 lm.PCODE,
                 cvu.FIN_PERIOD,
                 ROW_NUMBER()
                         OVER (PARTITION BY a.ENTERPRISE_CUSTOMER_ID order by a.ENTERPRISE_CUSTOMER_ID, cvu.FIN_PERIOD desc) as rank_period
          from CAT_redeemers as a
                   left join customer_analytics.production.cvu_customer_dim as cvu
                             on a.INFERRED_CUSTOMER_ID = cvu.EC_ID
       left join ADW_PROD.INC_PL.LOCATION_DIM as lm
        on cvu.PREFERRED_STORE = lm.LOCATION_KEY
         )
where rank_period = 1
        -- 2. limit to those sharing a postcode with a petrol store (non pay at pump)
and pcode in (select distinct PCODE from ADW_PROD.INC_PL.LOCATION_DIM where JS_PETROL_IND = 'Y') -- 'preffered stores' are specifically the superstore itself, but these share postcodes with the PFS attached
and preferred_store not in (2835,2341,2158,897,2271,2241,2240,2051,847,2304,2254,2301,2070,2400) -- remove pay at pump
;
select count(*) from pfs_fav_store; -- 672083





create or replace temp table pfs_fav_store2 as
    select distinct *, 'Y' as js_petrol_ind
    from (select a.*,
                 b.location,
                 b.pcode
          from ep_sg_m as a
                   left join EDWS_PROD.PROD_CMT_CAMPAIGN_01.TD_CUST_FAV_STORE as b
                             on a.ENTERPRISE_CUSTOMER_ID = b.ENTERPRISE_CUSTOMER_ID
         )
where location in (select distinct location from EDWS_PROD.PROD_CMT_PRESENTATION.VW_PETROL_MAIN_STORES)
and location not in (2835,2341,2158,897,2271,2241,2240,2051,847,2304,2254,2301,2070,2400) -- remove pay at pump
;
select count(*) from pfs_fav_store2; -- 677260




-- what volume of stretch & grow fit usual core criteria (shop in pfs)

-- fuel transactions

create or replace temp table fuel_spend_extract as
select distinct sel.*,
                fl.channel,
                fl.week_no,
                fl.fuel_litres,
                fl.fuel_spend
from ep_sg_m as sel
         inner join (select distinct
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
                      and dat.CALENDAR_DATE between dateadd(week, -12, current_date()) and current_date()

                        /*ONLY petrol category*/
                      and scat.sub_category = 839
                      and stl.unit_of_measure = 'L'
                      and lm.location not in (2835,2341,2158,897,2271,2241,2240,2051,847,2304,2254,2301,2070,2400) -- remove pay at pump
                    group by 1,2,3
) as fl
                   on sel.ENTERPRISE_CUSTOMER_ID = fl.ec_id

;

create or replace temp table fuel_spenders as
select *
from ep_sg_m
where enterprise_customer_id in (select distinct enterprise_customer_id from fuel_spend_extract) -- shopped fuel (non pay at pump) in past 12 weeks
;


select journey_instore, count(*) from fuel_spenders group by 1;


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
