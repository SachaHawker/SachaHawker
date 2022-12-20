USE ROLE RL_PROD_MARKETING_ANALYTICS;
USE DATABASE CUSTOMER_ANALYTICS;
USE WAREHOUSE WHS_PROD_MARKETING_ANALYTICS_XSMALL;
USE SCHEMA TD_REPORTING;

-- emailable and targeted
select count(*)
from TD09_2223_standard_dm a
    inner join EDWS_PROD.PROD_CMT_PRESENTATION.VW_SR_RT_SUPRESSION b
on a.sr_id = b.sr_id
where b.JSGROC_EM in ('Y','LI','LIN')
and a.target_control_flag =1;
--1,156,452


-- in suppression
select count(*)
from TD09_2223_standard_dm a
    inner join EDWS_PROD.PROD_CMT_PRESENTATION.VW_SR_RT_SUPRESSION b
on a.sr_id=b.sr_id;
--1,297,468

--Targetted
select count(*) from TD09_2223_petrol_dm where target_control_flag =1;
-- 1,167,753
