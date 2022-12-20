USE ROLE RL_PROD_MARKETING_ANALYTICS;
USE WAREHOUSE WHS_PROD_MARKETING_ANALYTICS_LARGE;
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA TD_REPORTING;

-- Draw out SR_ID and full nectar card no. associated with hashed loyalty ID
create or replace temp table bank_base as
    select a.SR_ID, b.hashed_loyalty_id, FULL_NECTAR_CARD_NUM, ec_id
        from "CUSTOMER_ANALYTICS"."DEVELOPMENT"."BANK_CREDIT_CARD_CUSTOMERS_WITH_FLAG" as b
        left join EDWS_PROd.PROD_CMT_PRESENTATION.VW_CA_CUSTOMER_ACCOUNT as a
            on sha2(cast(a.party_account_no as varchar(50)), 256) = b.hashed_loyalty_id
        where b.bank_segment in ('GO','PL') and b.points_flag = '1';


create or replace temp table bank_card_customers_DN_registered as
select a.*,
       b.weeks_since_latest_activity,
       b.grouped_segmentation
from Bank_base as a
inner join "EDWS_PROD"."PROD_CMT_CAMPAIGN_01"."DIG_NECTAR_SEGMENTATION" as b
on a.ec_id = b.ec_id;


create or replace temp table DN_active_instore as
select distinct ec_id,
       '419048' as Campaign_ID
from bank_card_customers_DN_registered;

create or replace temp table DN_active_online as
select distinct ec_id,
       '419047' as Campaign_ID
from bank_card_customers_DN_registered;
--cccccccc

Create or replace table DN_active_bank_pot as
    select ec_id,
           campaign_id
from DN_active_instore
union all
  select ec_id,
           campaign_id
               from DN_active_online
order by ec_id;
--32,730

create or replace table DN_active_bank_pot as
select
sha2(cast(ec_id as varchar(50)), 256) as ec_id,
       campaign_id
from DN_active_bank_pot
order by ec_id;

select * from ADW_PROD.ADW_CUSTOMER_PL.DIM_GROUP_CUSTOMER limit 5;