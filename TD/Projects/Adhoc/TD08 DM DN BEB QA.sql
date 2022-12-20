USE ROLE RL_PROD_MARKETING_ANALYTICS;
USE DATABASE CUSTOMER_ANALYTICS;
USE WAREHOUSE WHS_PROD_MARKETING_ANALYTICS_XSMALL;
USE SCHEMA TD_REPORTING;

select * from td08_2223_standard_dm limit 5;

--create table of our targets
create or replace temp table target_customers as
    select * from td08_2223_standard_dm
    where target_control_flag =1;

select count(distinct ec_id) from target_customers;
--916,051

--See if they signed up to digital Nectar, we have the choice of these 3 tables but I chose v2 since it is the most easy to use and has the correct dates
--v1 has wrong dates, v3 has an encrypted ec_id

select * from edws_prod.PROD_CMT_CAMPAIGN_01.DIG_NECTAR_SEGMENTATION; --v1
select * from edws_prod.PROD_CMT_CAMPAIGN_01.CVU_JS_DIGITAL_NECTAR_SEGMENTATION_LATEST; --v2
select * from ADW_PROD.ADW_FEATURES_MODELS_OUTPUTS.MODEL_JS_DIGITAL_NECTAR_SEGMENTATION
         where valid_to = '9999-12-31' limit 5; --v3

--we go with v2


--we want to select customers who were selected for the dm but were not dn reg. at the time
-- Join this to current dn customers table to see now if they are a part of it
create or replace temp table dn_customers_to_check_v2 as
select a.ec_id,
       a.full_nectar_card_num,
       b.sainsburys_customer,
       b.new_customer,
       b.active_outside_of_registration,
       b.weeks_since_latest_activity,
       b.grouped_segmentation
from target_customers as a
inner join  edws_prod.PROD_CMT_CAMPAIGN_01.CVU_JS_DIGITAL_NECTAR_SEGMENTATION_LATEST as b
on a.ec_id = b.ec_id
where a.ec_id in (select ec_id from td08_2223_standard_dm
                                 where grouped_segmentation is null);

--count of how many people
select count(*), count(distinct ec_id) from dn_customers_to_check_v2;
--7722

--are they all new customers?
select new_customer,
       count(ec_id),
       count(distinct ec_id)
from dn_customers_to_check_v2
group by 1;
-- 341 are not so we need an extra condition (but they may have signed up right at the beginning so we use the extra check below)
--since this campaign started 6 weeks ago this weeds out anyone who used it before
select count(*)
from dn_customers_to_check_v2
where new_customer <> 'false'
and weeks_since_latest_activity <= 6;
--5675

select new_customer,
       count(ec_id),
       count(distinct ec_id)
from dn_customers_to_check_v2
where new_customer <> 'false'
and weeks_since_latest_activity <= 6
group by 1;


select count(*)
from dn_customers_to_check_v2
where new_customer <> 'false';

--remove those who aren't new customers
create or replace temp table beb_custs as
select distinct
       full_nectar_card_num,
       sainsburys_customer,
       new_customer,
       active_outside_of_registration,
       weeks_since_latest_activity,
       grouped_segmentation
from dn_customers_to_check_v2
where new_customer <> 'false'
and weeks_since_latest_activity <= 6;

Select count (*), count (distinct full_nectar_card_num) from beb_custs;

--points for account and formatting for beb file
create or replace temp table beb_amounts as
select distinct ec_id,
                substring(full_nectar_card_num,len(full_nectar_card_num)-10,len(full_nectar_card_num))  as party_account_no,
                full_nectar_card_num as rewardcardnumber,
                500 as points_reward,
                points_reward*0.00425 as points_cost,
                current_date() as beb_date
from beb_custs
;

set transdate = (to_char(dateadd(days, -1, current_timestamp), 'DD/MM/YYYY HH:SS:MI'));

select count(*), count(distinct full_nectar_card_num) from beb_custs;
select count(*), count(distinct ec_id) from beb_custs;

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


select count(*) from TD10_2223_Standard_DM where SR_ID like '200%';