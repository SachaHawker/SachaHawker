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
Create or replace temp table Bank_card_customers_in_RF as
    select
           a.sr_id,
           b.enterprise_customer_ID,
           b.segment,
           a.hashed_loyalty_id,
           b.instore_accounts,
           b.online_accounts,
           c.journey_instore,
           b.party_account_type_code,
           case when b.party_account_id is not null then 1
               when b.party_account_id is null then 0 end as Ranking_File_Flag
from bank_base as a
left outer join EDWS_PROD.PROD_CMT_CAMPAIGN_01.RANKING_FILE_SR_NODUPES as b on b.PARTY_ACCOUNT_ID=a.SR_ID
    left join TD_REPORTING.TD_EXPERIMENTAL_JOURNEYS as c
    on b.enterprise_customer_ID = c.enterprise_customer_ID;


select count (sr_id) from Bank_card_customers_in_RF where Ranking_File_Flag = '1';

create or replace table bank_card_customers_8wk_DN_active as
select a.*,
       b.weeks_since_latest_activity,
       b.grouped_segmentation,
       case when b.weeks_since_latest_activity <= '8' then CAST('less_than_8wk' AS varchar)
           when b.weeks_since_latest_activity > '8' then CAST('over_8wk' AS varchar)
           end as DN_8_wk_flag
from Bank_card_customers_in_RF as a
Left join "EDWS_PROD"."PROD_CMT_CAMPAIGN_01"."DIG_NECTAR_SEGMENTATION" as b
on a.enterprise_customer_id = b.ec_id;


select count (ec_id) from bank_card_customers_8wk_DN_active where  DN_8_wk_flag = 'less_than_8wk';


create or replace temp table bank_card_customers_DN_registered as
select a.*,
       b.weeks_since_latest_activity,
       b.grouped_segmentation
from Bank_base as a
inner join "EDWS_PROD"."PROD_CMT_CAMPAIGN_01"."DIG_NECTAR_SEGMENTATION" as b
on a.ec_id = b.ec_id;



create or replace temp table DN_active_instore as
select ec_id,
       '419048' as Campaign_ID
from bank_card_customers_DN_registered;

create or replace temp table DN_active_online as
select ec_id,
       '419047' as Campaign_ID
from bank_card_customers_DN_registered;
--16,365

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




create or replace temp table bank_card_customers_transactions as
select weeks_since_latest_activity,
       grouped_segmentation,
       sr_id,
           enterprise_customer_ID,
           segment,
           hashed_loyalty_id,
           instore_accounts,
           online_accounts,
           journey_instore,
           party_account_type_code,
       DN_8_wk_flag,
       Ranking_File_Flag,
       (select case when max(b.transaction_date) < DATEADD(WEEK, -26, CURRENT_DATE()) then 'more than 26w'
           when max(b.transaction_date) >= DATEADD(day, -5, CURRENT_DATE()) then 'recent_transaction'
           else 'probably GOL' end as transaction_flag
        from bank_card_customers_8wk_DN_active as a
        inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_SHOPPING_TRANSACTION as b on a.SR_ID=b.PARTY_ACCOUNT_ID
        where a.ranking_file_flag = 0) as transaction_flag
from bank_card_customers_8wk_DN_active
group by 1,2,3,4,5,6,7,8,9,10,11,12;





select count(*) from bank_card_customers_transactions where transaction_flag = 'more than 26w';





-- those that aren't in RF
create or replace temp table Bank_customers_not_in_RF as
    select a.*
    from bank_base a
    left join Bank_card_customers_in_RF as b on a.SR_ID=b.party_account_id
    where b.party_account_id is null;


-- look at the last shop of those that are not in the RF
create or replace temp table bank_card_not_in_RF_last_shop as
    select a.SR_ID, max(TRANSACTION_DATE) as last_trx,
       case when last_trx < DATEADD(WEEK, -26, CURRENT_DATE()) then 'more than 26w'
           when last_trx >= DATEADD(day, -5, CURRENT_DATE()) then 'recent_transaction'
    else 'probably GOL' end as flag
    from Bank_customers_not_in_RF as a
    inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_SHOPPING_TRANSACTION as b on a.SR_ID=b.PARTY_ACCOUNT_ID
    group by 1;



-- online only
create or replace temp table bank_card_GOL as
    select a.*, d.SR_ID as GOL_SR_ID, G_26WK_TOTVALUE, G_LASTORDERDATE
    from Bank_customers_not_in_RF as a
    left join bank_card_not_in_RF_last_shop c on a.SR_ID = c.SR_ID
    inner join EDWS_PROD.PROD_CMT_PRESENTATION.VW_CA_CUSTOMER_ACCOUNT as d on a.FULL_NECTAR_CARD_NUM=d.FULL_NECTAR_CARD_NUM
    inner join EDWS_PROD.PROD_CMT_PRESENTATION.VW_CA_OL_TRANS_SUMMARY as e on d.SR_ID=e.SR_ID
    where c.SR_ID is null
        and d.PARTY_ACCOUNT_TYPE_CODE='02';


create or replace temp table bank_card_GOL_1 as
select case when G_26WK_TOTVALUE > 100 then 'GOL' else 'no' end as gol_flag, *
from bank_card_GOL;

select * from bank_card_GOL_1;

select count(distinct SR_ID), gol_flag

from bank_card_GOL_1
group by 2;


select * from "CUSTOMER_ANALYTICS"."TD_REPORTING"."SALES_REPORT_REPORTING_HISTORY" limit 10;

//WEEKLY SPEND/VISITS
create or replace temp table cc_bank_TRANS as
    select a.SR_ID
    ,c.week_num
    , COUNT(DISTINCT TRANSACTION_NUMBER) AS trans
    , sum(transaction_value) as spend
    , count(transaction_value) as countspend
    from bank_base as a
    inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_SHOPPING_TRANSACTION as b on a.SR_ID=b.PARTY_ACCOUNT_ID
    left join "ADW_PROD"."ADW_REFERENCE_PL"."DIM_WEEK" as c
    on b.transaction_date between WEEK_COMMENCING_DATE and WEEK_ENDING_DATE
    where transaction_date > '2022-01-01'
    group by 1, 2;


create or replace table bank_customers_tracking as
select  week_num
        ,sum(trans) as transactions
        ,sum(spend) as spend,
       sum(countspend) as total_visits
from    cc_bank_TRANS
group by week_num
;



select count(*) from Bank_card_customers_in_RF;
--18254 bank customers in ranking file


select * from Bank_card_customers_in_RF;


create or replace table bank_card_customers_8wk_DN_active as;


select * from "EDWS_PREPROD"."PROD_CMT_CAMPAIGN_01"."DIG_NECTAR_REGISTERED";  where ec_id = '50000029998783';



select * from ADW_PROD.ADW_CUSTOMER_PL.DIM_LOYALTY_SERVICE_CUSTOMER limit 5;





--
-- select a.*,
--        b.weeks_since_latest_activity,
--        b.grouped_segmentation
-- from Bank_card_customers_in_RF as a
-- inner join "EDWS_PROD"."PROD_CMT_CAMPAIGN_01"."DIG_NECTAR_SEGMENTATION" as b
-- on a.enterprise_customer_id = b.ec_id;
-- -- in DIG_NECTAR_SEG



