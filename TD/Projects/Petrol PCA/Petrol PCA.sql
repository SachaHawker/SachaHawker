USE ROLE RL_PROD_MARKETING_ANALYTICS;
USE WAREHOUSE WHS_PROD_MARKETING_ANALYTICS_LARGE;
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA TD_REPORTING;


-- how many Target / control
Select segment, count(distinct ec_ID) from "CUSTOMER_ANALYTICS"."TD_REPORTING"."TD02_2223_PETROL_DM" where target_control_flag = '0' group by segment;

-- TD08 Target: 267920 (Inactive:33487 Infrequent:183793 Lapsed:50640) Control: 14071 (Inactive:1731 Infrequent:9717 Lapsed:2623)
-- TD09 Target:  (Inactive:75966 Infrequent:300325 Lapsed:100349) Control: (Inactive:4072 Infrequent:15787 Lapsed:5269)
-- TD10 Target:  (Inactive:33537 Infrequent:184001 Lapsed:50842) Control: (Inactive:1763 Infrequent:9786 Lapsed: 2574)
-- TD11 Target:  (Inactive:33753 Infrequent:185240 Lapsed:51050) Control: (Inactive:1773 Infrequent:9807 Lapsed:2690)
-- TD01 Target:  (Inactive:46359 Infrequent:162090  Lapsed:60857) Control: (Inactive:5032 Infrequent:18183 Lapsed:6716)
-- TD02 Target:  (Inactive:31456 Infrequent: 183141 Lapsed:49524) Control: (Inactive:3474 Infrequent:20477 Lapsed:5385)


-- selection file
Select * from "CUSTOMER_ANALYTICS"."TD_REPORTING"."TD08_2122_PETROL_DM";




-- -- no. of red by week
-- select fin_week, sum(redeem_qty)
-- from TD10_pet where EC_id in (select distinct ec_id from TD10_2122_PETROL_DM where Target_control_flag = 1)
-- group by 1 order by 1;
--
-- select * from TD10_2122_PETROL_DM limit 5;

-- -- test no dupes
-- select count(concat(ec_id, transaction_number)), count(distinct(concat(ec_id, transaction_number))) from petrol;
-- -- count:404,920
-- -- Count Distinct:20,246


create or replace table Petrol_PCA_redemptions as
    select * from TD08_red
Union select * from TD09_red
Union select * from TD10_red
Union Select * from TD11_red
Union select * from TD01_red
Union select * from TD02_red;


-- create or replace table Petrol_PCA_barcodes as
--     select * from TD08
-- Union select * from TD09
-- Union select * from TD10
-- Union Select * from TD11
-- Union select * from TD01
-- Union select * from TD02;
--
-- select * from Petrol_PCA_barcodes;

select * from Petrol_PCA_redemptions;

select count (distinct concat (ec_id, transaction_number)) from Petrol_PCA_redemptions;

select campaignname,fin_week, sum(redeem_qty)
from Petrol_PCA_redemptions where EC_id in (select distinct ec_id from TD02_2223_PETROL_DM where Target_control_flag = 1)
and campaignname like '2223_TD02'
and petrol_barcode is not null
group by 1,2 order by 2;


select count(*) from Petrol_PCA_redemptions where EC_id in (select distinct ec_id from TD10_2122_PETROL_DM where Target_control_flag = 1)
and fin_week = 202144 and EC_id in (select distinct ec_id from TD09_2122_PETROL_DM where Target_control_flag = 1)


-- print tables
create or replace table Petrol_prints as
select distinct pf.party_account_id as sr_id,
                pf.transaction_date,
                pf.transaction_time,
                case when left(cast(sr_id as varchar(15)), 1) = '4' then '04'
                     when left(cast(sr_id as varchar(15)), 1) = '2' then '02' else '99' end as party_account_type_code,
                pf.location,
                bl.barcode,
                bl.petrol_barcode,
                pf.print_qty,
                0 as redeem_qty,
                bl.campaignname
from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_CATALINA_PRINT_FILE  as pf
         inner join Petrol_barcode_lookup as bl
                    on substr(pf.barcode,1,7) = substr(bl.petrol_Barcode,7,7)
                    and pf.transaction_date between bl.campaign_start_date and bl.campaign_end_date
;


-- joining to ec_id
create or replace temp table Petrol_PCA_Redemptions_Prints2 as
    select  b.enterprise_customer_id as ec_id, a.*
from Petrol_PCA_Redemptions_Prints as a
left join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_PARTY_ACCOUNT as b
on a.sr_id = b.party_account_id
;

-- add redemptions and prints together
Create or replace table Petrol_PCA_Redemptions_Prints as
    select a.*, b.print_qty from Petrol_PCA_redemptions as a
left join Petrol_prints
on a.sr_id = b.Sr_id;


