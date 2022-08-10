USE ROLE RL_PROD_MARKETING_ANALYTICS;
USE WAREHOUSE WHS_PROD_MARKETING_ANALYTICS_LARGE;
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA TD_REPORTING;

create or replace temp table ranking_file as
    select
          substr(enterprise_customer_ID,4,11) as ec_id,
           party_account_id,
           party_account_no,
           segment
from EDWS_PROD.PROD_CMT_CAMPAIGN_01.RANKING_FILE_SR_NODUPES;


Create or replace temp table Card_customers as
    select
           a.party_account_no,
           a.segment,
           b.hashed_loyalty_id
from EDWS_PROD.PROD_CMT_CAMPAIGN_01.RANKING_FILE_SR_NODUPES as a
inner join bank_credit_card as b
on sha2(cast(a.party_account_no as varchar(50)), 256) = b.hashed_loyalty_id;


select count(*)
from "EDWS_PROD"."PROD_CMT_PRESENTATION"."VW_CA_CUSTOMER_ACCOUNT"  as a
inner join bank_credit_card as b
on b.hashed_loyalty_ID = sha2(cast(a.party_account_no as varchar(50)), 256)

select * from "EDWS_PROD"."PROD_CMT_CAMPAIGN_01"."RANKING_FILE_SR_202221";

select count(distinct party_account_no) from Card_customers;
-- those in CC data that are in CURRENT the ranking file: 511,633
-- those in CC data that are in LAST WEEKS ranking file: 511,709

select count(distinct hashed_loyalty_id) from bank_credit_card;
-- 744,299

-- Matching bank card on customer account: 744,082 -- 217 missing -- deactivated bank/nectar?

select count(party_account_no), segment from Card_customers group by 2;
-- 151282,Regular
-- 34097,SuperFrequent
-- 11513,Missing
-- 122479,Infrequent
-- 106808,Frequent
-- 45486,Lapsed
-- 39968,Inactive





