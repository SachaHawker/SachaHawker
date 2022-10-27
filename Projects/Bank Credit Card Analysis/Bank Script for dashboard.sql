-- WEEKLY SPEND/VISITS
create or replace temp table cc_bank_TRANS as
    select a.SR_ID
    ,c.week_num
    , COUNT(DISTINCT TRANSACTION_NUMBER) AS trans
    , sum(transaction_value) as spend

    from bank_base as a
    inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_SHOPPING_TRANSACTION as b on a.SR_ID=b.PARTY_ACCOUNT_ID
    left join "ADW_PROD"."ADW_REFERENCE_PL"."DIM_WEEK" as c
    on b.transaction_date between WEEK_COMMENCING_DATE and WEEK_ENDING_DATE
    where transaction_date > '2022-01-01'
    group by 1, 2;
-- 838 have shopping transactions

select  week_num
        ,sum(trans)
        ,sum(spend)
from    cc_bank_TRANS
group by week_num
;
