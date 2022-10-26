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

select * from bank_base;

-- Check they all match
select SR_ID from bank_base order by 1;
select count(*), count(distinct SR_ID), count(distinct hashed_loyalty_id) from bank_base;
--matching 19338

--STEP 2: CREATING JOINED TABLES (BANK CUSTOMERS AND RANKING FILE)//
-- This is only for Gold and Platnium customers with 10k or over points in last 12m and have an instore account //
Create or replace temp table cc_Card_customers_13_9_22_RF as
    select
           a.party_account_id,
           a.enterprise_customer_ID,
           a.segment,
           b.hashed_loyalty_id,
           a.instore_accounts,
           a.online_accounts,
           c.journey_instore,
           a.party_account_type_code
from EDWS_PROD.PROD_CMT_CAMPAIGN_01.RANKING_FILE_SR_NODUPES as a
inner join bank_base as b on a.PARTY_ACCOUNT_ID=b.SR_ID
    left join TD_REPORTING.TD_EXPERIMENTAL_JOURNEYS as c
    on a.enterprise_customer_ID = c.enterprise_customer_ID
where a.party_account_type_code='04';

select count(*) from cc_Card_customers_13_9_22_RF;
--18254 bank customers in ranking file


-- // WORK OUT WHO IS NOT IN THE RANKING FILE //
create or replace temp table cc_not_in_rf as
    select a.*
    from bank_base a
    left join cc_Card_customers_13_9_22_RF as b on a.SR_ID=b.party_account_id
    where b.party_account_id is null;

select count(*) from cc_not_in_rf;
--1084

-- look at the last shop of those that are not in the RF
create or replace temp table cc_bank_last_shop as
    select a.SR_ID, max(TRANSACTION_DATE) as last_trx
    from cc_not_in_rf as a
    inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_SHOPPING_TRANSACTION as b on a.SR_ID=b.PARTY_ACCOUNT_ID
    group by 1;
-- 838 have shopping transactions


-- see when the transactions have been
create or replace temp table cc_bank_split1 as
select *,
       case when last_trx < '2022-03-15' then 'more than 26w'
            when last_trx > '2022-09-10' then 'check trx before' else 'probably GOL' end as flag
from cc_bank_last_shop
;


select count(*), flag from cc_bank_split1 group by 2;
/*
 COUNT(*),FLAG
14,check trx before
824,more than 26w
 */

 select * from cc_bank_split1 where flag = 'check trx before';

//checking valid items
select a.party_account_id, c.sku_desc, a.transaction_date, a.party_account_type_code
from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_SHOPPING_TRANSACTION_LINE a
    inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_EAN as b
on a.ean_key = b.ean_key
inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_SKU_MAP as c
on b.sku_key = c.sku_key
where party_account_id = '40059290243015'
and transaction_date  = '2022-09-12';


--now check the rest
create or replace temp table cc_bank_gol as
    select a.*, d.SR_ID as GOL_SR_ID, G_26WK_TOTVALUE, G_LASTORDERDATE
    from cc_not_in_rf as a
    left join cc_bank_split1 c on a.SR_ID = c.SR_ID
    inner join EDWS_PROD.PROD_CMT_PRESENTATION.VW_CA_CUSTOMER_ACCOUNT as d on a.FULL_NECTAR_CARD_NUM=d.FULL_NECTAR_CARD_NUM
    inner join EDWS_PROD.PROD_CMT_PRESENTATION.VW_CA_OL_TRANS_SUMMARY as e on d.SR_ID=e.SR_ID
    where c.SR_ID is null
        and d.PARTY_ACCOUNT_TYPE_CODE='02';

select * from cc_bank_gol;

select count(*), count(distinct SR_ID) from cc_bank_gol;

create or replace temp table cc_bank_gol_1 as
select case when G_26WK_TOTVALUE > 100 then 'GOL' else 'no' end as gold_flag, *
from cc_bank_gol;

select * from cc_bank_gol_1

select count(distinct SR_ID), gold_flag
from cc_bank_gol_1
group by 2;
/*
 COUNT(DISTINCT SR_ID),GOLD_FLAG
120,GOL
26,no
 */

-- //what's missing - all people that are not in RF, not in shopping transactions and not in online
create or replace temp table cc_bank_where1 as
    select a.*
from cc_not_in_rf as a
where a.SR_ID not in (select sr_id from cc_bank_split1)
    and SR_ID not in (select distinct SR_ID
           from cc_bank_gol_1
           where gold_flag='GOL');

select count(*) from cc_bank_where1;

select * from cc_bank_where1;

// add on EC_id
create or replace temp table cc_bank_where2 as
    select a.* from cc_bank_where1 a
inner join EDWS_PROd.PROD_CMT_PRESENTATION.VW_CA_CUSTOMER_ACCOUNT as b
on a.sr_id = b.sr_id;

select count(distinct SR_ID) from cc_bank_where2;



// get loyalty segment for those not in transaction customers
create or replace temp table cc_bank_where_SOW1 as
Select *
From (
select a.SR_ID,
       a.ec_id,
b.loyalty_seg,
row_number() over (partition by SR_ID ORDER BY WEEK_no desc) as row_no
 from cc_bank_where2 a
     left join ADW_PROD.ADW_PL.LOYALTY_SEGMENTS b
         on sha2(cast(a.ec_id as varchar(50)), 256)  = b.enterprise_customer_num
) where row_no = 1;

select * from cc_bank_where_SOW1 -- 126
select * from TD_REPORTING.cc_bank_where_SOW1 where loyalty_seg in ('GO','PL') --31 // GO & PL cusotmers //
select * from TD_REPORTING.cc_bank_where_SOW1 where loyalty_seg not in ('GO','PL') --26 // Not GO & PL cusotmers//
select * from TD_REPORTING.cc_bank_where_SOW1 where loyalty_seg is null -- 69 // Null customers //

-- //Example: Get EC_ID from SOW table (those that apparently don't have any transactions //
-- select customer_key from ADW_PROD.INC_PL.customer_DIM where inferred_customer_id = sha2(cast( '50000021045491' as varchar(50)), 256)
--
-- // Plug this into two different transaction tables //
-- select * from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_SHOPPING_TRANSACTION where party_account_id ='40061933195032'
-- -- no transactions
-- Select * from ADW_PROD.INC_PL.TRANSACTION_DIM where customer_key = '31dd1474b22d23983ad7f3e82c3a70582eeeffd2d8c95e7d1ef5927abe4d9162';
-- -- transactions in the last 26 weeks
--
-- // Table of those that supposedly haven't had transactions //
Create or replace temp table transactions as
select c.*, a.loyalty_seg, a.ec_id, a.sr_id
from TD_REPORTING.cc_bank_where_SOW1 a
inner join ADW_PROD.INC_PL.customer_DIM b
on sha2(cast(a.ec_id as varchar(50)), 256) = b.inferred_customer_ID
inner join ADW_PROD.INC_PL.TRANSACTION_DIM c
on b.customer_key = c.customer_key;

select count(distinct customer_key) from transactions -- 94 have had transactions

create or replace temp table transactions_in_26w as
select distinct customer_key, loyalty_seg, ec_id, sr_id from transactions where date_key >= '2022-03-15';
-- 17 customers have shopped in last 26 weeks

-- // these customers haven't shopped in the shopping transaction table but have in transaction dim table //
select a.ec_id, a.loyalty_seg
from transactions_in_26w as a
inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_SHOPPING_TRANSACTION as b
on a.sr_id = b.party_account_id;

select * from ADW_PROD.INC_PL.TRANSACTION_DIM limit 5;