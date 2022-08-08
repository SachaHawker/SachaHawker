USE ROLE RL_PROD_MARKETING_ANALYTICS;
USE WAREHOUSE WHS_PROD_MARKETING_ANALYTICS_LARGE;
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA TD_REPORTING;

Select * from EDWS_PROD.PROD_CMT_CAMPAIGN_01.RANKING_FILE_SR_NODUPES limit 5;

select count (enterprise_customer_id) from EDWS_PROD.PROD_CMT_CAMPAIGN_01.RANKING_FILE_SR_NODUPES;

select * from Bank_credit_card limit 5;

create or replace temp table ranking_file as
    select
          substr(enterprise_customer_ID,4,11) as ec_id,
           party_account_id,
           party_account_no,
           segment
from EDWS_PROD.PROD_CMT_CAMPAIGN_01.RANKING_FILE_SR_NODUPES;

Create or replace temp table Card_customers as
    select
           a.ec_id,
           a.party_account_id,
           a.party_account_no,
           a.segment,
           b.hashed_loyalty_id
from ranking_file as a
inner join bank_credit_card as b
on sha2(cast(a.ec_id as varchar(50)), 256) = b.hashed_loyalty_id;

select count(distinct ec_id) from Card_customers;
-- those in CC data that are in the ranking file: 129

select count(distinct hashed_loyalty_id) from bank_credit_card;
-- 744,299

select count(ec_id), segment from Card_customers group by 2;
-- 38,Regular
-- 5,SuperFrequent
-- 1,Missing
-- 33,Infrequent
-- 20,Frequent
-- 14,Lapsed
-- 18,Inactive


select ec_id,
       sha2(cast(ec_id as varchar(50)), 256) as hashed,
       segment
       from ranking_file;

