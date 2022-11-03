USE ROLE RL_PROD_MARKETING_ANALYTICS;
USE WAREHOUSE WHS_PROD_MARKETING_ANALYTICS_LARGE;
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA TD_REPORTING;

Create or replace table SSUT_TD09_2223_DM as
select * from CUSTOMER_ANALYTICS.SANDBOX.LA_XMAS22_FINAL
where should_shop = 1 and xmas_shop_propensity <='0.5';
-- 1,214,831

select count(*) from CUSTOMER_ANALYTICS.SANDBOX.LA_XMAS22_FINAL a
inner join EDWS_PROD.PROD_CMT_CAMPAIGN_01.RANKING_FILE_SR_NODUPES b
on a.inferred_customer_id = sha2(cast(b.enterprise_customer_ID as varchar(50)), 256)
where a.should_shop = 1 and a.xmas_shop_propensity <='0.5'
--13,671,696 total overlap
-- with where clauses 1,240,267

