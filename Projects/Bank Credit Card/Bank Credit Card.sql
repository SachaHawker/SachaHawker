USE ROLE RL_PROD_MARKETING_ANALYTICS;
USE WAREHOUSE WHS_PROD_MARKETING_ANALYTICS_LARGE;
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA TD_REPORTING;

// STEP 1:  Create ranking file with hashed ID //
create or replace table Hashed_RF as
    select party_account_no,
           sha2(cast(party_account_no as varchar(50)), 256) as hashed_loyalty_id,
           party_account_type_code,
           instore_accounts,
           online_accounts
from EDWS_PROD.PROD_CMT_CAMPAIGN_01.RANKING_FILE_SR_NODUPES;
select * from hashed_RF;



// STEP 2: CREATING JOINED TABLES (BANK CUSTOMERS AND RANKING FILE)//
// This is only for Gold and Platnium customers with 10k or over points in last 12m and have an instore account //
Create or replace table Card_customers_13_9_22_RF as
    select
           a.party_account_no,
           a.enterprise_customer_ID,
           a.segment,
           b.hashed_loyalty_id,
           b.bank_segment,
           b.points_flag,
           a.instore_accounts,
           a.online_accounts,
           c.journey_instore,
           a.party_account_type_code
from EDWS_PROD.PROD_CMT_CAMPAIGN_01.RANKING_FILE_SR_NODUPES as a
inner join "CUSTOMER_ANALYTICS"."DEVELOPMENT"."BANK_CREDIT_CARD_CUSTOMERS_WITH_FLAG" as b
on sha2(cast(a.party_account_no as varchar(50)), 256) = b.hashed_loyalty_id
    left join TD_REPORTING.TD_EXPERIMENTAL_JOURNEYS as c
    on a.enterprise_customer_ID = c.enterprise_customer_ID
where b.bank_segment in ('GO','PL') and b.points_flag = '1' and a.party_account_type_code='04';

select count(*) from Card_customers_13_9_22_RF;
--18254 bank customers in ranking file

select count(*) from "CUSTOMER_ANALYTICS"."DEVELOPMENT"."BANK_CREDIT_CARD_CUSTOMERS_WITH_FLAG"
where bank_segment in ('GO','PL') and points_flag = '1';
--19338 - all bank customers



// WORK OUT WHO IS NOT IN THE RANKING FILE //
-- not in the ranking file --
create or replace table Not_in_Ranking_file as
select hashed_loyalty_id
from "CUSTOMER_ANALYTICS"."DEVELOPMENT"."BANK_CREDIT_CARD_CUSTOMERS_WITH_FLAG"
where hashed_loyalty_ID not in (select hashed_loyalty_id from Card_customers_13_9_22_RF)
and bank_segment in ('GO', 'PL') and points_flag=1;
-- 1,084 not in the ranking file

//those that have shopped over 26 weeks ago//
create or replace temp table shopped_over_26weeks as
select a.hashed_loyalty_id, b.Party_account_no, b.party_account_type_code, c.transaction_date
from Not_in_Ranking_file as a
inner join "EDWS_PROD"."PROD_CMT_PRESENTATION"."VW_CA_CUSTOMER_ACCOUNT" as b
on a.hashed_loyalty_id = sha2(cast(b.party_account_no as varchar(50)), 256)
inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_SHOPPING_TRANSACTION_LINE as c
on a.hashed_loyalty_id = sha2(cast(substr(c.party_account_id,4,11) as varchar(50)), 256)
where transaction_date <= '2022-03-15';
-- 844 bank people have transactions greater than 26 weeks and therefore not in the ranking file


// Find out what are the other customers that are not in RF and not an over 26 week shopper //
create or replace temp table other_customers as
select a.hashed_loyalty_id, b.*
from Not_in_Ranking_file as a
inner join  EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_PARTY_ACCOUNT as b
on a.hashed_loyalty_id = sha2(cast(b.party_account_no as varchar(50)), 256)
where hashed_loyalty_id not in (select hashed_loyalty_id from shopped_over_26weeks)
-- 240 not a over 26 week shopper and not in the ranking file

select * from other_customers;

// see if the 'other customers' group have shopped at all //
Select a.hashed_loyalty_id
from Not_in_Ranking_file as a
inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_PARTY_ACCOUNT as b
on a.hashed_loyalty_id = sha2(cast(b.party_account_no as varchar(50)), 256)
where party_account_id in (select party_account_id from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_shopping_transaction)
-- the 'other_cusotmers' group haven't shopped at all


// check those that not in the ranking file, yet shopped in the last 26 weeks //
select a.hashed_loyalty_id, b.Party_account_no, b.party_account_type_code, c.transaction_date
from Not_in_Ranking_file as a
inner join "EDWS_PROD"."PROD_CMT_PRESENTATION"."VW_CA_CUSTOMER_ACCOUNT" as b
on a.hashed_loyalty_id = sha2(cast(b.party_account_no as varchar(50)), 256)
inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_SHOPPING_TRANSACTION as c
on a.hashed_loyalty_id = sha2(cast(substr(c.party_account_id,4,11) as varchar(50)), 256)
where transaction_date >= '2022-03-15';
-- 8 people shopped in the last 26 weeks yet not in the ranking file (due to 99 party account code and excluded items)


//DN EXCLUSIONS//
--all DN registered for EP
--8 week DN active for core

// those that have less than 8 weeks activity //
create or replace temp table DN_8w_active as
select count(distinct hashed_loyalty_id) as count,
       hashed_loyalty_id
from Card_customers_13_9_22_RF
where hashed_loyalty_id in
      (select sha2(cast(b.party_account_no as varchar(50)), 256) as hashed_loyalty_id
      from "EDWS_PROD"."PROD_CMT_CAMPAIGN_01"."DIG_NECTAR_SEGMENTATION" as a
      inner join EDWS_PROD.PROD_CMT_CAMPAIGN_01.RANKING_FILE_SR_NODUPES as b
      on a.ec_id = b.enterprise_customer_id
          where a.weeks_since_latest_activity <= '8')
group by 2;
--11,610

select count(*) from DN_8w_active;

// those we target in core //
create or replace temp table core_target as
select count(distinct hashed_loyalty_id) as count,
       segment
from Card_customers_13_9_22_RF
where hashed_loyalty_id not in
      (select sha2(cast(b.party_account_no as varchar(50)), 256) as hashed_loyalty_id
      from "EDWS_PROD"."PROD_CMT_CAMPAIGN_01"."DIG_NECTAR_SEGMENTATION" as a
      inner join EDWS_PROD.PROD_CMT_CAMPAIGN_01.RANKING_FILE_SR_NODUPES as b
      on a.ec_id = b.enterprise_customer_id
          where a.weeks_since_latest_activity <= '8')
group by 2;

select * from core_target;



// those that are DN active //
create or replace temp table DN_active as
select count(distinct hashed_loyalty_id) as count
from Card_customers_13_9_22_RF
where hashed_loyalty_id in
      (select sha2(cast(substr(sr_ID,4,11) as varchar(50)), 256) as hashed_loyalty_id
      from "EDWS_PROD"."PROD_CMT_CAMPAIGN_01"."REGISTERED_HASHED_SRID");
--15585

select * from DN_active;


// those that we target from EP//

create or replace temp table EP_target as
select count(distinct hashed_loyalty_id) as count,
       journey_instore
from Card_customers_13_9_22_RF
where hashed_loyalty_id not in
(select sha2(cast(substr(sr_ID,4,11) as varchar(50)), 256) as hashed_loyalty_id
      from "EDWS_PROD"."PROD_CMT_CAMPAIGN_01"."REGISTERED_HASHED_SRID")
group by 2;

select * from EP_target;
--2626


// Should be EP pot //

create or replace temp table should_be_in_EP as
select count(distinct hashed_loyalty_id) as count,
       segment,
       journey_instore
from Card_customers_13_9_22_RF
where hashed_loyalty_id not in
       (select sha2(cast(b.party_account_no as varchar(50)), 256) as hashed_loyalty_id
      from "EDWS_PROD"."PROD_CMT_CAMPAIGN_01"."DIG_NECTAR_SEGMENTATION" as a
      inner join EDWS_PROD.PROD_CMT_CAMPAIGN_01.RANKING_FILE_SR_NODUPES as b
      on a.ec_id = b.enterprise_customer_id)
  and journey_instore not in ('Maintain','Stretch & Grow','Reactivate')
and segment not in ('Inactive', 'Lapsed', 'Infrequent')
group by 2,3;

select * from should_be_in_EP;


//// BY SEGMENT // GOLD & PLATNIUM
select count(party_account_no), segment from Card_customers_06_9_22_RF where segment in ('Infrequent','Lapsed','Inactive') group by 2;
select count(party_account_no),journey_instore from Card_customers_06_9_22_RF group by 2;

// BY SEGMENT // GOLD
select count(party_account_no), segment from Card_customers_06_9_22_RF where bank_segment = 'GO' group by 2;
// EP Journey //
select count(party_account_no),journey_instore from CCard_customers_06_9_22_RF where bank_segment = 'GO' group by 2;

// BY SEGMENT // PLATNIUM
select count(party_account_no), segment from Card_customers_06_9_22_RF where bank_segment = 'PL' group by 2;
select count(party_account_no),journey_instore from Card_customers_06_9_22_RF where bank_segment = 'PL' group by 2;

