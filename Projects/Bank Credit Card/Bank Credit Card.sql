USE ROLE RL_PROD_MARKETING_ANALYTICS;
USE WAREHOUSE WHS_PROD_MARKETING_ANALYTICS_LARGE;
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA TD_REPORTING;


//CREATING JOINED TABLES (BANK CUSTOMERS AND RANKING FILE)//
// This is only for Gold and Platinum customers with 10k or over points in last 12m //
Create or replace temp table Card_customers_23_8_22_RF as
    select
           a.party_account_no,
           a.enterprise_customer_ID,
           a.segment,
           b.hashed_loyalty_id,
           b.bank_segment,
           b.points_flag,
           a.instore_accounts,
           c.journey_instore
from EDWS_PROD.PROD_CMT_CAMPAIGN_01.RANKING_FILE_SR_NODUPES as a
inner join "CUSTOMER_ANALYTICS"."DEVELOPMENT"."BANK_CREDIT_CARD_CUSTOMERS_WITH_FLAG" as b
on sha2(cast(a.party_account_no as varchar(50)), 256) = b.hashed_loyalty_id
    left join TD_REPORTING.TD_EXPERIMENTAL_JOURNEYS as c
    on a.enterprise_customer_ID = c.enterprise_customer_ID
where bank_segment in ('GO','PL') and points_flag = '1' and instore_accounts = '1';


-- those in bank data that are in CURRENT the ranking file:
select count(distinct party_account_no) from Card_customers_23_8_22_RF;-- 17937


//// BY SEGMENT // GOLD & PLATNIUM

select count(party_account_no), segment from Card_customers_23_8_22_RF group by 2;
-- 88,Missing
-- 703,Infrequent
-- 175,Inactive
-- 285,Lapsed
-- 3985,Regular
-- 9373,Frequent
-- 3328,SuperFrequent

select count(party_account_no),journey_instore from Card_customers_23_8_22_RF group by 2;
-- 324,Reactivate
-- 1,N/A
-- 77,Not enough transactions
-- 309,Stretch & Grow
-- 1908,Maintain
-- 15318, Null



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

-- select * from EDWS_PROD.PROD_CMT_CAMPAIGN_01.RANKING_FILE_SR_NODUPES limit 50;
-- select * from  TD_REPORTING.TD_EXPERIMENTAL_JOURNEYS limit 5;
-- select count(*) from "CUSTOMER_ANALYTICS"."DEVELOPMENT"."BANK_CREDIT_CARD_CUSTOMERS_WITH_FLAG" where bank_segment in ('GO','PL') and points_flag = 1
