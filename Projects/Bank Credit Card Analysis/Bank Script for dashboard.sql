USE ROLE RL_PROD_MARKETING_ANALYTICS;
USE WAREHOUSE WHS_PROD_MARKETING_ANALYTICS_LARGE;
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA TD_REPORTING;

//Find SR_ID and full nectar card no. associated with hashed loyalty ID from bank table
-- This is only for Gold and Platnium customers with 10k or over points in last 12m and have an instore account
create or replace temp table bank_base as
    select a.SR_ID, b.hashed_loyalty_id, FULL_NECTAR_CARD_NUM, ec_id, a.party_account_type_code
        from "CUSTOMER_ANALYTICS"."DEVELOPMENT"."BANK_CREDIT_CARD_CUSTOMERS_WITH_FLAG" as b
        left join EDWS_PROd.PROD_CMT_PRESENTATION.VW_CA_CUSTOMER_ACCOUNT as a
            on sha2(cast(a.party_account_no as varchar(50)), 256) = b.hashed_loyalty_id
        where b.bank_segment in ('GO','PL') and b.points_flag = '1';

select * from bank_base;

-- Check they all match
select SR_ID from bank_base order by 1;
select count(*), count(distinct ec_id), count(distinct hashed_loyalty_id) from bank_base;
--matching 19338


//Find all customers that are in the ranking file and add flag
Create or replace temp table Bank_card_customers_in_RF as
    select
           a.sr_id,
           a.ec_id,
           a.hashed_loyalty_id,
           c.journey_instore,
           a.party_account_type_code,
           a.FULL_NECTAR_CARD_NUM,
           case when ec_id in (select enterprise_customer_ID from EDWS_PROD.PROD_CMT_CAMPAIGN_01.RANKING_FILE_SR_NODUPES) then 1
               else 0
               end as Ranking_File_Flag
from bank_base as a
    left join TD_REPORTING.TD_EXPERIMENTAL_JOURNEYS as c
    on a.ec_id = c.enterprise_customer_ID;


//Finding all customers that are less than 8 weeks active
create or replace table bank_card_customers_8wk_DN_active1 as
select *
from "EDWS_PROD"."PROD_CMT_CAMPAIGN_01"."DIG_NECTAR_SEGMENTATION" as b
where weeks_since_latest_activity <='8';

//Adding in DN flag to main pot
create or replace table bank_card_customers_8wk_DN_active2 as
select *,
       case when ec_id in (select ec_id from bank_card_customers_8wk_DN_active1) then '8wk Active'
         else 'Over 8wk Active'
        end as DN_8_wk_flag
from Bank_card_customers_in_RF;



// All customers that have shopped online
Create or replace temp table  bank_card_customers_GOL as
    select a.*, d.SR_ID as GOL_SR_ID, G_26WK_TOTVALUE, G_LASTORDERDATE
    from bank_card_customers_8wk_DN_active2 as a
    inner join EDWS_PROD.PROD_CMT_PRESENTATION.VW_CA_CUSTOMER_ACCOUNT as d on a.FULL_NECTAR_CARD_NUM=d.FULL_NECTAR_CARD_NUM
    inner join EDWS_PROD.PROD_CMT_PRESENTATION.VW_CA_OL_TRANS_SUMMARY as e on d.SR_ID=e.SR_ID
        where d.PARTY_ACCOUNT_TYPE_CODE='02';

--checking numbers still match
select count (distinct sr_id), count(*) from bank_card_customers_GOL;

//Finding customers that are online//
create or replace temp table bank_card_customers_GOL_1 as
    select distinct sr_id, ec_id, sum( G_26WK_TOTVALUE) as total_value
from bank_card_customers_GOL
    group by 1,2
having total_value >100;

//Adding in GOL flag to main pot//
create or replace temp table bank_card_customers_GOL_2 as
    select *,
           case when ec_id in (select ec_id from bank_card_customers_GOL_1) then 'GOL' else 'Not GOL' end as GOL_Flag
from bank_card_customers_8wk_DN_active2;





//WEEKLY SPEND/VISITS
-- By transaction, so can have more than one row per customer
create or replace table cc_bank_TRANS as
 select a.SR_ID,
        a.ec_id
    ,c.week_num
    , COUNT(DISTINCT concat(b.TRANSACTION_NUMBER, b.transaction_time)) AS trans
    , sum(b.transaction_value) as spend,
           DN_8_wk_flag,
           Ranking_File_Flag,
           gol_flag
    from  bank_card_customers_GOL_2 as a
    left join (select * from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_SHOPPING_TRANSACTION  where transaction_date >= '2022-01-01') as b
        on a.SR_ID=b.PARTY_ACCOUNT_ID
    left join "ADW_PROD"."ADW_REFERENCE_PL"."DIM_WEEK" as c
    on b.transaction_date between WEEK_COMMENCING_DATE and WEEK_ENDING_DATE
    group by 1,2,3,6,7,8;



//Final Output table//
-- gives customers by week and flags (transaction)
create or replace table bank_customers_tracking as
select  week_num,
        DN_8_wk_flag,
        Ranking_File_Flag,
        gol_flag
        ,sum(spend) as spend,
       sum(trans) as visits,
       count(distinct sr_id) as Customers
from    cc_bank_TRANS
group by 1,2,3,4
;

// Trying to track customers by week but due to needing week it includes transactions
// How is it done where it will update each week with the ranking file and that will give the week (not transaction week)
Create or replace table Bank_CC_Customers1 as
    select  count(distinct ec_id) as count,
                    Ranking_File_Flag,
                    gol_flag,
                    DN_8_wk_flag,
           week_num
from cc_bank_TRANS group by 2,3,4,5;


Create or replace table Bank_CC_Customers as
    select  sum(count) as count,
                    Ranking_File_Flag,
                    gol_flag,
                    DN_8_wk_flag,
                    week_num
from Bank_CC_Customers1 group by 2,3,4,5;

select count, DN_8_wk_flag from Bank_CC_Customers group by 1,2;
select week_num from Bank_CC_Customers;

--
-- select count(*) from Bank_card_customers_in_RF;
-- --18254 bank customers in ranking file
--
--
-- select * from Bank_card_customers_in_RF;
--
--
-- create or replace table bank_card_customers_8wk_DN_active as;
--
--
-- select * from "EDWS_PREPROD"."PROD_CMT_CAMPAIGN_01"."DIG_NECTAR_REGISTERED";  where ec_id = '50000029998783';
--
--
--
-- select * from ADW_PROD.ADW_CUSTOMER_PL.DIM_LOYALTY_SERVICE_CUSTOMER limit 5;
--
--
--
--
--
-- --
-- -- select a.*,
-- --        b.weeks_since_latest_activity,
-- --        b.grouped_segmentation
-- -- from Bank_card_customers_in_RF as a
-- -- inner join "EDWS_PROD"."PROD_CMT_CAMPAIGN_01"."DIG_NECTAR_SEGMENTATION" as b
-- -- on a.enterprise_customer_id = b.ec_id;
-- -- -- in DIG_NECTAR_SEG
--
--
--
--
-- -- -- those that aren't in RF
-- -- create or replace temp table Bank_customers_not_in_RF as
-- --     select a.*
-- --     from bank_base a
-- --     left join Bank_card_customers_in_RF as b on a.SR_ID=b.party_account_id
-- --     where b.party_account_id is null;
-- --
-- --
-- -- -- look at the last shop of those that are not in the RF
-- -- create or replace temp table bank_card_not_in_RF_last_shop as
-- --     select a.SR_ID, max(TRANSACTION_DATE) as last_trx,
-- --        case when last_trx < DATEADD(WEEK, -26, CURRENT_DATE()) then 'more than 26w'
-- --            when last_trx >= DATEADD(day, -5, CURRENT_DATE()) then 'recent_transaction'
-- --     else 'probably GOL' end as flag
-- --     from Bank_customers_not_in_RF as a
-- --     inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_SHOPPING_TRANSACTION as b on a.SR_ID=b.PARTY_ACCOUNT_ID
-- --     group by 1;
--
--


-- create or replace temp table bank_card_customers_transactions as
-- select a.weeks_since_latest_activity,
--        a.grouped_segmentation,
--        a.sr_id,
--            a.enterprise_customer_ID,
--           a.segment,
--            a.hashed_loyalty_id,
--            a.instore_accounts,
--            a.online_accounts,
--            a.journey_instore,
--            a.party_account_type_code,
--        a.DN_8_wk_flag,
--        a.Ranking_File_Flag,
--        a.FULL_NECTAR_CARD_NUM,
--        case when a.ranking_file_flag = 0 then
--                                 case when max(b.transaction_date) < DATEADD(WEEK, -26, CURRENT_DATE()) then 'more than 26w'
--                                 when max(b.transaction_date) >= DATEADD(day, -5, CURRENT_DATE()) then 'recent_transaction'
--                                 else 'probably GOL' end
--            when a.ranking_file_flag = 1 then null else null end as Transaction_flag
-- from bank_card_customers_8wk_DN_active a
--  left join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_SHOPPING_TRANSACTION as b on a.SR_ID=b.PARTY_ACCOUNT_ID
-- group by 1,2,3,4,5,6,7,8,9,10,11,12,13;


-- create or replace temp table bank_card_customers_GOL_1 as
-- select case when G_26WK_TOTVALUE > 100
--     then 'GOL'
--     else 'Not GOL'
--     end as gol_flag,
--        *
-- from bank_card_customers_GOL;
--
-- select count(distinct SR_ID), gol_flag
-- from bank_card_customers_GOL_1
-- group by 2;
--
-- select * from bank_card_customers_GOL_1 where sr_id in (select sr_id from bank_card_customers_transactions2);
--
-- select count (*) from bank_card_customers_GOL_1;
--
--
-- Create or replace temp table bank_card_customers_Online as
--     select a.*,
--            ifnull(b.gol_flag,'Not GOL') as GOL
-- from bank_card_customers_transactions2 a
-- left join bank_card_customers_GOL_1 b
-- on a.sr_id = b.sr_id;
--
-- select * from bank_card_customers_transactions2;
-- --19338
-- select count(distinct sr_id), count(distinct enterprise_customer_ID), gol  from bank_card_customers_Online group by 3;
-- -- 19338
--
-- select * from bank_card_customers_Online where gol_flag is not null;
--
-- create or replace temp table bank_card_customers_transactions as
--     select a.ec_id, max(TRANSACTION_DATE) as last_trx
-- from bank_card_customers_8wk_DN_active2 a
--     inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_SHOPPING_TRANSACTION as b on a.SR_ID=b.PARTY_ACCOUNT_ID
-- where Ranking_File_Flag = 0
--    group by 1;
-- --816
--
--
-- -- see when the transactions have been
-- create or replace temp table bank_card_customers_transactions1 as
-- select *,
--        case when last_trx < DATEADD(WEEK, -26, CURRENT_DATE()) then 'more than 26w'
--             when last_trx >= DATEADD(day, -5, CURRENT_DATE())  then 'recent transaction'
--            else 'probably GOL' end as transaction_flag
-- from bank_card_customers_transactions
-- ;
--
-- select count(*), transaction_flag from bank_card_customers_transactions1 group by 2;
--
-- create or replace temp table bank_card_customers_transactions2 as
--     select a.*,
--            b.transaction_flag
-- from bank_card_customers_8wk_DN_active a
-- left join bank_card_customers_transactions1 b
-- on a.sr_id=b.sr_id;
--
-- select * from bank_card_customers_transactions2 where transaction_flag is not null;

-- create or replace table bank_card_customers_8wk_DN_active as
-- select a.*,
--        b.weeks_since_latest_activity,
--        b.grouped_segmentation,
--        case when b.weeks_since_latest_activity <= '8' then CAST('less_than_8wk' AS varchar)
--            when b.weeks_since_latest_activity > '8' then CAST('over_8wk' AS varchar)
--            end as DN_8_wk_flag
-- from Bank_card_customers_in_RF as a
-- Left join "EDWS_PROD"."PROD_CMT_CAMPAIGN_01"."DIG_NECTAR_SEGMENTATION" as b
-- on a.enterprise_customer_id = b.ec_id;

-- Create or replace temp table Bank_card_customers_in_RF as
--     select
--            a.sr_id,
--            b.enterprise_customer_ID,
--            b.segment,
--            a.hashed_loyalty_id,
--            b.instore_accounts,
--            b.online_accounts,
--            c.journey_instore,
--            b.party_account_type_code,
--            a.FULL_NECTAR_CARD_NUM,
--            case when b.party_account_id is not null then 1
--                when b.party_account_id is null then 0 end as Ranking_File_Flag
-- from bank_base as a
-- left outer join EDWS_PROD.PROD_CMT_CAMPAIGN_01.RANKING_FILE_SR_NODUPES as b on b.PARTY_ACCOUNT_ID=a.SR_ID
--     left join TD_REPORTING.TD_EXPERIMENTAL_JOURNEYS as c
--     on b.enterprise_customer_ID = c.enterprise_customer_ID;