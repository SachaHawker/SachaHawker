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

