USE ROLE RL_PROD_MARKETING_ANALYTICS;
USE DATABASE CUSTOMER_ANALYTICS;
USE WAREHOUSE WHS_PROD_MARKETING_ANALYTICS_LARGE;
USE SCHEMA PRODUCT_REPORTING;

select * from EDWS_PROD.PROD_CMT_CAMPAIGN_01.SNP_NITESTCT_POP where NI_CUST = 1;

Select distinct segment from EDWS_PROD.PROD_CMT_CAMPAIGN_01.SNP_BEAUTY_C09_CAT_QA;

Select * from EDWS_PROD.PROD_CMT_CAMPAIGN_01.SNP_BEAUTY_C09_CAT_QA;

--PPT1
select * from CUSTOMER_ANALYTICS.PRODUCT_REPORTING.PRODUCT_PCA_REPOSITORY where campaign_code = 'BC09CT';

--PPT2
select * from "CUSTOMER_ANALYTICS"."PRODUCT_REPORTING"."PRODUCT_PPT2_PCA_REPOSITORY" where campaign_code = 'BC09CT';

--PPT3
select * from "CUSTOMER_ANALYTICS"."PRODUCT_REPORTING"."PRODUCT_PPT3_PCA_REPOSITORY" where campaign_code = 'BC09CT';

--PPT4
select * from "CUSTOMER_ANALYTICS"."PRODUCT_REPORTING"."PRODUCT_PPT4_PCA_REPOSITORY" where campaign_code = 'BC09CT';

-- all redemptions
select * from beautyc6_Redemptions;

select distinct barcode from beautyc6_Redemptions where reward_value_fixed is not null;

select * from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_shopping_transaction limit 10;


create or replace table vw_payment as
select party_account_id,
       party_account_type_code,
       transaction_date,
       transaction_time,
       transaction_number,
       payment_value,
       coupon_id,
       location_key,
       payment_type_code,
       till_number
from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_payment_line
where coupon_id <> ''
;

Create or replace temp table beauty_pounds as
select distinct pa.enterprise_customer_id as ec_id,
                pl.party_account_id as sr_id,
                pl.transaction_date,
                to_time(right(cast(1000000 + pl.transaction_time as varchar(7)), 6)) as transaction_time,
                pl.transaction_number,
--                 concat(pl.transaction_number,pl.transaction_time,pl.transaction_date) as transaction_identifier,
                st.party_account_type_code,
                l.location,
                st.transaction_value,
                bl.barcode,
                pl.payment_value
from vw_payment as pl
         inner join (select * from beautyc6_redemptions) as bl
                    on (bl.barcode = pl.coupon_id
                        and pl.transaction_date = bl.red_date)
         inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_shopping_transaction st
                    on st.location_key             = pl.location_key
                        and pl.transaction_date         = st.transaction_date
                        and st.transaction_time        = pl.transaction_time
                        and st.transaction_number      = pl.transaction_number
                        and st.party_account_type_code = pl.party_account_type_code
                        and st.party_account_id        = pl.party_account_id
                        and st.till_number             = pl.till_number
         inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_c_location_map as l
                    on l.location_key             = st.location_key
         left join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_party_account as pa
                   on pa.party_account_id        = st.party_account_id
where pl.payment_type_code       = '003';


select count (*) from beauty_pounds;
--3755

create or replace temp table beauty_points as
select distinct
    pa.enterprise_customer_id as ec_id,
                point_red.party_account_id as sr_id,
                point_red.transaction_date,
                to_time(right(cast(1000000 + point_red.transaction_time as varchar(7)), 6)) as transaction_time,
                point_red.transaction_number,
                st.party_account_type_code,
                l.location,
                st.transaction_value,
                bl.barcode,
                point_red.points_earned*0.00425 as payment_value
from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_loyalty_coupon_redemption as point_red
         inner join (select * from beautyc6_redemptions) as bl
                    on substring(bl.barcode ,8 ,7) = point_red.coupon_id
                        and reward_value_fixed is not null
                        and length(bl.barcode)=15
                        and point_red.transaction_date = bl.red_date

         left join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_shopping_transaction as st
                    on st.location_key            = point_red.location_key
                        and point_red.transaction_date = case when st.ACTUAL_TRANSACTION_DATE_DELTA = 0 then st.transaction_date
                                                              when st.ACTUAL_TRANSACTION_DATE_DELTA = -1 then (st.transaction_date-1) end
                        and st.transaction_time        = point_red.transaction_time
                        and st.transaction_number      = point_red.transaction_number
                        and st.party_account_type_code = point_red.party_account_type_code
                        and st.party_account_id        = point_red.party_account_id
                        and st.till_number             = point_red.till_number

         left join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_party_account as pa
                   on pa.party_account_id        = st.party_account_id
  left join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_c_location_map as l
                    on l.location_key             = st.location_key
;

select * from beauty_points;
select count(*) from beauty_points;
--10886

create or replace temp table beauty_all_redemptions as
select distinct * from beauty_points
union all
select distinct * from beauty_pounds;

select count(*) from beauty_all_redemptions order by transaction_time asc ;
--14641

select count(*) from beautyc6_redemptions;
--15147

select * from beautyc6_redemptions;

select count(*) from beautyc6_redemptions where reward_value_fixed is not null;
--points 11393
-- 10886

select count(*) from beautyc6_redemptions where reward_value_money is not null;
-- Money 3754
--3755