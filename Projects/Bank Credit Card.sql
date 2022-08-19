USE ROLE RL_PROD_MARKETING_ANALYTICS;
USE WAREHOUSE WHS_PROD_MARKETING_ANALYTICS_LARGE;
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA TD_REPORTING;


//CREATING JOINED TABLES (BANK CUSTOMERS AND RANKING FILE)//
Create or replace temp table Card_customers_9_8_22_RF as
    select
           a.party_account_no,
           a.segment,
           b.hashed_loyalty_id
from EDWS_PROD.PROD_CMT_CAMPAIGN_01.RANKING_FILE_SR_NODUPES as a
inner join bank_credit_card as b
on sha2(cast(a.party_account_no as varchar(50)), 256) = b.hashed_loyalty_id;

Create or replace temp table Card_customers_2_8_22_RF as
    select
           a.party_account_no,
           a.segment,
           b.hashed_loyalty_id
from "EDWS_PROD"."PROD_CMT_CAMPAIGN_01"."RANKING_FILE_SR_202221" as a
inner join bank_credit_card as b
on sha2(cast(a.party_account_no as varchar(50)), 256) = b.hashed_loyalty_id;

select count(distinct party_account_no) from Card_customers_9_8_22_RF;
-- those in CC data that are in CURRENT the ranking file: 511,633
select count(distinct party_account_no) from Card_customers_2_8_22_RF;
-- those in CC data that are in LAST WEEKS ranking file: 511,709

// BY SEGMENT //
select count(party_account_no), segment from Card_customers_9_8_22_RF group by 2;
-- 151282,Regular
-- 34097,SuperFrequent
-- 11513,Missing
-- 122479,Infrequent
-- 106808,Frequent
-- 45486,Lapsed
-- 39968,Inactive

select count(party_account_no), segment from Card_customers_2_8_22_RF group by 2;
-- 34599,SuperFrequent
-- 11554,Missing
-- 123141,Infrequent
-- 40145,Inactive
-- 44600,Lapsed
-- 150522,Regular
-- 107148,Frequent



// COUNTS //

-- Counts in the bank file
select count(distinct hashed_loyalty_id) from bank_credit_card;
-- 744,299

-- counts of bank customers that are in the customer account
select count(*)
from "EDWS_PROD"."PROD_CMT_PRESENTATION"."VW_CA_CUSTOMER_ACCOUNT"  as a
inner join bank_credit_card as b
on b.hashed_loyalty_ID = sha2(cast(a.party_account_no as varchar(50)), 256)

-- Matching bank card on customer account: 744,082 -- 217 missing -- deactivated bank/nectar?




select * from "EDWS_PROD"."PROD_CMT_CAMPAIGN_01"."RANKING_FILE_SR_202221" where segment = 'Missing'; -- last weeks RF (2nd Aug)

select * from EDWS_PROD.PROD_CMT_CAMPAIGN_01.RANKING_FILE_SR_NODUPES limit 50;