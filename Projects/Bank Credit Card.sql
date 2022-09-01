USE ROLE RL_PROD_MARKETING_ANALYTICS;
USE WAREHOUSE WHS_PROD_MARKETING_ANALYTICS_LARGE;
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA TD_REPORTING;


select count(*) from "CUSTOMER_ANALYTICS"."DEVELOPMENT"."BANK_CREDIT_CARD_CUSTOMERS_WITH_FLAG"
where hashed_loyalty_id in
      (select inferred_customer_ID
      from customer_analytics.production.cvu_digital_nectar_engagement_segmentation
          where weeks_since_latest_activity <= '8');



select * from "EDWS_PROD"."PROD_CMT_CAMPAIGN_01"."DIG_NECTAR_SEGMENTATION" limit 5;

select * from customer_analytics.production.cvu_digital_nectar_engagement_segmentation limit 5;

-- RF with hashed ID
create or replace table Hashed_RF as
    select party_account_no,
           sha2(cast(party_account_no as varchar(50)), 256) as hashed_loyalty_id,
           party_account_type_code,
           instore_accounts
from EDWS_PROD.PROD_CMT_CAMPAIGN_01.RANKING_FILE_SR_NODUPES;


select count(*) from EDWS_PROD.PROD_CMT_CAMPAIGN_01.RANKING_FILE_SR_NODUPES where instore_accounts >=1 and party_account_type_code = '02';;

select * from EDWS_PROD.PROD_CMT_CAMPAIGN_01.RANKING_FILE_SR_NODUPES where party_account_type_code <> '04' limit 5;
select * from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_shopping_transaction where party_account_type_code <> '04' ;
select * from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_shopping_transaction where party_account_ID like '400%';

select * from EDWS_PROD.PROD_CMT_CAMPAIGN_01.RANKING_FILE_SR_NODUPES limit 5 ;



-- 04 party account types: 4th in and 11 long
-- 02 party account types: 6th in and 9 long (20000106320243 - 106320243)

-- those that are not in RF --
select * from "CUSTOMER_ANALYTICS"."DEVELOPMENT"."BANK_CREDIT_CARD_CUSTOMERS_WITH_FLAG"
where hashed_loyalty_id not in (select hashed_loyalty_id from Hashed_RF) and bank_segment in ('GO','PL') and points_flag=1;
-- 1093 from bank that are not in RF

select * from "CUSTOMER_ANALYTICS"."DEVELOPMENT"."BANK_CREDIT_CARD_CUSTOMERS_WITH_FLAG"
where hashed_loyalty_id ='b147b7137b72ee4f0d0259b12e9131d5c0fb613002436f9283e0db894a2f2b3c';



-- shopping transaction with hashed id --
create or replace table hashed_transaction_02 as
    select *,
           sha2(cast((substr(party_account_id,6,9)) as varchar(50)), 256) as hashed_ID
               from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_shopping_transaction;

hashed loyalty id, lastdidigts, instore its 400 + if its online gives onlien cust no


select * from hashed_online
where hashed_id ='b147b7137b72ee4f0d0259b12e9131d5c0fb613002436f9283e0db894a2f2b3c';




create or replace temp table hashed_online as
select loyalty_card_number,
       sha2(cast((substr(loyalty_card_number,9,11)) as varchar(50)), 256) as hashed_id
from "EDWS_PREPROD"."PROD_CMT_PRESENTATION"."VW_CA_ONLINE_CUSTOMER";

select * from "EDWS_PREPROD"."PROD_CMT_PRESENTATION"."VW_CA_ONLINE_CUSTOMER" limit 5;


select * from hashed_transaction_02
where hashed_id ='b147b7137b72ee4f0d0259b12e9131d5c0fb613002436f9283e0db894a2f2b3c'; -- 0

create or replace table hashed_transaction_04 as
    select *,
           sha2(cast(substr(party_account_id,4,11) as varchar(50)), 256) as hashed_ID
               from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_shopping_transaction;



select * from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_shopping_transaction where party_account_type_code = '02';


    select * from "CUSTOMER_ANALYTICS"."DEVELOPMENT"."BANK_CREDIT_CARD_CUSTOMERS_WITH_FLAG"
where hashed_loyalty_id not in (select hashed_loyalty_id from Hashed_RF) and bank_segment in ('GO','PL') and points_flag=1

-- not in RF 1093
-- sample not in there:
-- 'f4f9ac0555a3c317522d086e04b70114fe0fc1339b17c3de8cf0754c75671f26' -- feb 2021
--e4113a80766c033551ae2f258d681063e7b10c5b9c227f10eb0d3ef271bab602 -- March 2020
--b147b7137b72ee4f0d0259b12e9131d5c0fb613002436f9283e0db894a2f2b3c -- not in there
--847d8aeeea682cdd5f306152f5fe1d7118f5f87a8dafdfb7a2e9f03f50bf470a -- April 2021
--694431023a38d35a9196d0a8fa6fa8b9dabf32418584bd415147d22d5dade2c7 -- May 2021


select * from "CUSTOMER_ANALYTICS"."DEVELOPMENT"."BANK_CREDIT_CARD_CUSTOMERS_WITH_FLAG"
where hashed_loyalty_id ='b147b7137b72ee4f0d0259b12e9131d5c0fb613002436f9283e0db894a2f2b3c';

Create or replace temp table lookup as
    select a.*,
           b.instore_accounts,
           b.party_account_no,
           b.sha2(cast(party_account_no as varchar(50)), 256) as hashed_loyalty_id,
           b.party_account_type_code
    from "CUSTOMER_ANALYTICS"."DEVELOPMENT"."BANK_CREDIT_CARD_CUSTOMERS_WITH_FLAG" as a
    left join EDWS_PROD.PROD_CMT_CAMPAIGN_01.RANKING_FILE_SR_NODUPES as b
on a.hashed_loyalty_ID = b.hashed_loyalty_id;









//CREATING JOINED TABLES (BANK CUSTOMERS AND RANKING FILE)//
// This is only for Gold and Platnium customers with 10k or over points in last 12m and have an instore account //
Create or replace table Card_customers_23_8_22_RF as
    select
           a.party_account_no,
           a.enterprise_customer_ID,
           a.segment,
           b.hashed_loyalty_id,
           b.bank_segment,
           b.points_flag,
           a.instore_accounts,
           c.journey_instore,
           a.party_account_type_code
from EDWS_PROD.PROD_CMT_CAMPAIGN_01.RANKING_FILE_SR_NODUPES as a
inner join "CUSTOMER_ANALYTICS"."DEVELOPMENT"."BANK_CREDIT_CARD_CUSTOMERS_WITH_FLAG" as b
on sha2(cast(a.party_account_no as varchar(50)), 256) = b.hashed_loyalty_id
    left join TD_REPORTING.TD_EXPERIMENTAL_JOURNEYS as c
    on a.enterprise_customer_ID = c.enterprise_customer_ID
where b.bank_segment in ('GO','PL') and b.points_flag = '1' and a.instore_accounts >=1;

select * from Card_customers_23_8_22_RF
select * from EDWS_PROD.PROD_CMT_CAMPAIGN_01.RANKING_FILE_SR_NODUPES limit 5;

-- those in bank data that are in CURRENT the ranking file:
select count(distinct party_account_no) from Card_customers_23_8_22_RF;
-- 17937 (1 instore account)
-- 18245 instore (may also have online account)
-- online only 0
-- instore only 10948
--instore and online 7297
select count(*) from "CUSTOMER_ANALYTICS"."DEVELOPMENT"."BANK_CREDIT_CARD_CUSTOMERS_WITH_FLAG" where bank_segment in ('GO','PL') and points_flag = '1';
--19338 - all bank customers


//// BY SEGMENT // GOLD & PLATNIUM

select count(party_account_no), segment from Card_customers_23_8_22_RF group by 2;
-- 4041,Regular
-- 9535,Frequent
-- 3406,SuperFrequent
-- 88,Missing
-- 712,Infrequent
-- 176,Inactive
-- 287,Lapsed

select count(party_account_no),journey_instore from Card_customers_23_8_22_RF group by 2;
-- 1,N/A
-- 74,Not enough transactions
-- 330,Reactivate
-- 15600, Null
-- 1919,Maintain
-- 321,Stretch & Grow




// BY SEGMENT // GOLD
select count(party_account_no), segment from Card_customers_23_8_22_RF where bank_segment = 'GO' group by 2;
-- 1642,Regular
-- 879,SuperFrequent
-- 34,Missing
-- 301,Infrequent
-- 3107,Frequent
-- 116,Lapsed
-- 72,Inactive

// EP Journey //
select count(party_account_no),journey_instore from Card_customers_23_8_22_RF where bank_segment = 'GO' group by 2;
--5309, Null
-- 95,Reactivate
-- 149,Stretch & Grow
-- 577,Maintain
-- 21,Not enough transactions


// BY SEGMENT // PLATNIUM
select count(party_account_no), segment from Card_customers_23_8_22_RF where bank_segment = 'PL' group by 2;
-- 2449,SuperFrequent
-- 54,Missing
-- 402,Infrequent
-- 103,Inactive
-- 169,Lapsed
-- 2343,Regular
-- 6266,Frequent

select count(party_account_no),journey_instore from Card_customers_23_8_22_RF where bank_segment = 'PL' group by 2;
-- 160,Stretch & Grow
-- 10009, Null
-- 229,Reactivate
-- 1,N/A
-- 1331,Maintain
-- 56,Not enough transactions



// Tables //

select * from EDWS_PROD.PROD_CMT_CAMPAIGN_01.RANKING_FILE_SR_NODUPES limit 50;
-- select * from  TD_REPORTING.TD_EXPERIMENTAL_JOURNEYS limit 5;
-- select count(*) from "CUSTOMER_ANALYTICS"."DEVELOPMENT"."BANK_CREDIT_CARD_CUSTOMERS_WITH_FLAG" where bank_segment in ('GO','PL') and points_flag = 1