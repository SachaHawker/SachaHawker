
USE ROLE MARKETING_CUSTOMER_ANALYTICS;
USE WAREHOUSE ADW_2XLARGE_ADHOC_WH;
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA TD_REPORTING;

--1. Exclusions
-- Active Digital Nectar Customers (EDWS_PROD.PROD_CMT_CAMPAIGN_01.DIG_NECTAR_SEGMENTATION)
-- Program level control group (EDWS_PROD.PROD_CMT_CAMPAIGN_01.TD_MASTER_CORE_PLC_FLAG)
-- Mailable (EDWS_PROD.PROD_CMT_PRESENTATION.VW_CA_CUSTOMER_ACCOUNT)
-- Null Postcodes (EDWS_PROD.PROD_CMT_PRESENTATION.VW_CA_CUSTOMER_ACCOUNT)
-- Orphan Accounts (EDWS_PROD.PROD_CMT_PRESENTATION.VW_CA_CUSTOMER_ACCOUNT)
-- Excluded addresses (ADDR_EXC_LIST)
-- Name Stop? (EITHER_NAME_STOP) - funny first name (swear word)
-- Deceased VW_EC_RT_SUPRESSION
-- Suppressed Address VW_EC_RT_SUPRESSION
-- DM Non-redeemers (standard only)

--2. Get Ranking File Segment
-- RANKING_FILE_SR_NODUPES matches selection

--3. Contactability

--4. Calculate Volumes

--Petrol customers between these weeks
set start_petrol_week = DATEADD(WEEK,-78, current_date);
set stop_petrol_week = current_date();

--1. Create Exclusion Tables

-- -- 26 week active DN Old method
-- create or replace temp table active26_hash as
-- SELECT distinct hashed_loyalty_id
-- FROM customer_analytics.sandbox.tmb_dnv_active
--      INNER JOIN (SELECT MAX(active_week) as max FROM customer_analytics.sandbox.tmb_dnv_active)
--      WHERE active_week > dateadd(WEEK,-26,max);
--
-- create or replace temp table digital_nectar_active as
-- select enterprise_customer_id as ec_id from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_party_account
-- where sha2(cast(party_account_no as varchar(50)), 256) in (select * from active26_hash);

--
create or replace temp table digital_nectar_active as
    SELECT distinct EC_ID
    FROM EDWS_PROD.PROD_CMT_CAMPAIGN_01.DIG_NECTAR_SEGMENTATION
    where WEEKS_SINCE_LATEST_ACTIVITY <=8;

create or replace temp table plc_exclusion as
    select enterprise_customer_id as ec_id
    from EDWS_PROD.PROD_CMT_CAMPAIGN_01.TD_MASTER_CORE_PLC_FLAG
    where PLC_FLAG =1;

create or replace temp table non_dmable_exclusion as
select enterprise_customer_id as ec_id from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_party_account
where party_account_id in
(select * from EDWS_PROD.PROD_CMT_CAMPAIGN_01.SNP_JSG_NON_DMABLE);

-- create or replace temp table mailable_exclusion as
-- select ec_id
-- from EDWS_PROD.PROD_CMT_PRESENTATION.VW_CA_CUSTOMER_ACCOUNT
-- where mailable = '0';

create or replace temp table postcode_null
    as
    select ec_id from EDWS_PROD.PROD_CMT_PRESENTATION.VW_CA_CUSTOMER_ACCOUNT
where postcode is null;

create or replace temp table orphan_exclusion as
select ec_id
from EDWS_PROD.PROD_CMT_PRESENTATION.VW_CA_CUSTOMER_ACCOUNT
where ORPHAN_ACCOUNT_IND = 'Y';

create or replace temp table address_exclusion as
select ENTERPRISE_CUSTOMER_ID as ec_id
from EDWS_PROD.PROD_CMT_CAMPAIGN_01.ADDR_EXC_LIST;

create or replace temp table either_name_stop_exclusion as
select EC_ID from
EDWS_PROD.PROD_CMT_CAMPAIGN_01.EITHER_NAME_STOP;

create or replace temp table deceased_exclusion as
    select EC_ID from EDWS_PROD.PROD_CMT_PRESENTATION.VW_EC_RT_SUPRESSION
WHERE QUINETICDECEASEDSUPPRFLAG in ('Y', null);

create or replace temp table suppressed_address_exclusion as
    select ec_id from EDWS_PROD.PROD_CMT_PRESENTATION.VW_EC_RT_SUPRESSION
WHERE ADDRESSSUPPRESSIONFLAG = 'Y';

create or replace temp table DM_exclusions as
    select * from digital_nectar_active
union
    select * from plc_exclusion
union
    select * from postcode_null
union
    select * from orphan_exclusion
union
    select * from address_exclusion
union
    select * from either_name_stop_exclusion
union
    select * from deceased_exclusion
union
    select * from suppressed_address_exclusion;
--

-- Get latest redemption Segmentation - to create exclusion table of non-redeemers
create or replace temp table TD_REDEMPTION_SEGMENTATION as
select inferred_customer_id,
       ec_id,
       overall_segment,
       dm_segment,
       ct_segment
from CUSTOMER_ANALYTICS.PRODUCTION.CVU_REDEMPTION_SEGMENTATION red
inner join EDWS_PROD.PROD_CMT_PRESENTATION.VW_CA_CUSTOMER_ACCOUNT ca
    on red.inferred_customer_id = sha2(cast(ca.ec_id as varchar(50)), 256);


create or replace temp table non_redeemers as
select inferred_customer_id,
       ec_id,
       overall_segment,
       dm_segment,
       ct_segment
from EDWS_PROD.PROD_CMT_CAMPAIGN_01.CVU_REDEMPTION_SEGMENTATION
where DM_SEGMENT = 'non redeemer';


--2: Select the ranking file customers

create or replace temp table ranking_file_srid as
    select segment,
           enterprise_customer_id,
           party_account_no,
           PARTY_ACCOUNT_ID,
           sha2(cast(party_account_no as varchar(50)), 256) as INFERRED_SR_ID
    from EDWS_PROD.PROD_CMT_CAMPAIGN_01.RANKING_FILE_SR_NODUPES
where segment in ('Inactive', 'Infrequent', 'Lapsed');

select segment, count(distinct enterprise_customer_id) as ranking_file from ranking_file_srid group by 1 order by 1;
--3,686,569

create or replace table ranking_file_exclusions as
select * from ranking_file_srid
where enterprise_customer_id not in
(select ec_id from DM_exclusions)
and concat('98263000',party_account_no) in
          (select distinct FULL_NECTAR_CARD_NUM from EDWS_PROD.PROD_CMT_PRESENTATION.VW_CA_CUSTOMER_ACCOUNT
                               where LMS_STATUS_CODE in ('A', 'AC'));
-- without mailable 4,039,364
-- with mailable 914,366

-- -- 3: Apply contactability inclusion

--Look at who is contactable by DM
create or replace temp table contactable as
    select * from EDWS_PROD.PROD_CMT_PRESENTATION.vw_sr_rt_supression
        where JSGROC_DM in ('Y','LI','LIN');

-- Join this onto the ranking file
create or replace temp table ranking_file_contactable as
    select *
        from ranking_file_exclusions
            where enterprise_customer_id in
                (select ec_id from contactable);

select segment, count(distinct enterprise_customer_id) as contactable from ranking_file_contactable group by 1 order by 1;
--01/11 1,795,643

--5. Are they an eligible petrol customer

--Gather these customers' fuel spend data
create or replace temp table contactable_fuel_flag as
    select a.*, fl.fuel_litres, fl.fuel_spend, case when fl.fuel_litres > 0 and fl.fuel_spend >0 then 1 else 0 end as fuel_flag
           from ranking_file_contactable as a
left join (select distinct
                        pa.enterprise_customer_id as ec_id,
                        pa.party_account_id as sr_id,
                        case when stl.party_account_type_code = '04' then 'Instore'
                             when stl.party_account_type_code = '02' then 'Online' end as channel,
                        dat.week_no,
                        sum(stl.item_weight) as fuel_litres,
                        sum(stl.extended_price) as fuel_spend

                    from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_shopping_transaction_line as stl,
                         EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_c_location_map as lm,
                         EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_ean as ean,
                         EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_c_sku_map as sku,
                         EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_c_sub_category_map as scat,
                         EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_payment_line as pay,
                         EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_DATE_MAP as dat,
                         EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_PARTY_ACCOUNT as pa


                    where stl.location_key = lm.location_key
                      and lm.js_petrol_ind <> 'N'
                      and stl.ean_key = ean.ean_key
                      and ean.sku_key = sku.sku_key
                      and sku.sub_category_key = scat.sub_category_key
                      and stl.party_account_id=pay.party_account_id
                      and stl.party_account_type_code=pay.party_account_type_code
                      and stl.transaction_date=pay.transaction_date
                      and stl.transaction_time=pay.transaction_time
                      and stl.transaction_number=pay.transaction_number
                      and stl.location_key=pay.location_key
                      and stl.till_number=pay.till_number
                      and dat.calendar_date=stl.transaction_date
                      and pa.party_account_id=stl.party_account_id

                      and dat.calendar_date between $start_petrol_week and $stop_petrol_week

                        /*ONLY petrol category*/
                      and scat.sub_category = 839
                      and stl.unit_of_measure = 'L'
                    group by 1,2,3,4
) as fl
on a.party_account_id = fl.sr_id;

-- Create an exclusion customers of those who bought fuel - excluding certain locations
create or replace temp table fuel_customers as
    select distinct enterprise_customer_id
        from contactable_fuel_flag
            where fuel_flag = 1
            and enterprise_customer_id not in
                (select ENTERPRISE_CUSTOMER_ID
                from EDWS_PROD.PROD_CMT_CAMPAIGN_01.USR_TD_PREF_STORE
                where LOCATION in (2240,2070,2254,2158,2271,2301,2241,2051,897,847,2304,2835));

-- Split out Fuel Customers
create or replace temp table DM_split as
    select *, case when enterprise_customer_id in
                (select enterprise_customer_id from fuel_customers) then 'Petrol' else 'Standard' end as campaign,
           case when enterprise_customer_id in
    (select distinct EC_ID
    FROM EDWS_PROD.PROD_CMT_CAMPAIGN_01.DIG_NECTAR_SEGMENTATION) then 'DN' else 'Non-DN' end as DN_flag
        from ranking_file_contactable
where not (campaign = 'Standard' and enterprise_customer_id in (select ec_id from non_redeemers));


-- create or replace temp table week_52_customers as
-- select distinct enterprise_customer_id, campaign, segment, DN_flag as DM_split from DM_split
-- where campaign = 'Petrol'
-- group by 1,2,3,4 order by 2,3,4;


create or replace temp table week_72_customers as
select distinct enterprise_customer_id, campaign, segment, DN_flag as DM_split from DM_split
where campaign = 'Petrol'
group by 1,2,3,4 order by 2,3,4;

Create or replace temp table Petrol_52_minus_26_customers as
    select enterprise_customer_id, campaign, segment, DM_split from week_52_customers
where enterprise_customer_id not in (select enterprise_customer_id from week_26_customers);

select count (*) from Petrol_52_minus_26_customers;

-- 14/10 1204582
-- 01/11 1130634
-- 02/11 1167525

select campaign,
       segment,
       count(distinct enterprise_customer_id),
          dn_flag
from DM_split
where campaign = 'Petrol'
group by 1,2,4 order by 1,2;






--
-- -- Tests
--
-- select count(ec_id) from TD03_2223_STANDARD_DM
-- where ec_id in (select enterprise_customer_id from DM_split)
-- --404903 out of 831622 48%
--
-- select count(ec_id) from TD03_2223_PETROL_DM
-- where ec_id in (select enterprise_customer_id from DM_split)
-- --182036 out of 274408 66%
--
-- --
-- -- select count(ec_id) from TD03_2223_STANDARD_DM
-- -- where EC_ID in (select ec_id from TD02_2223_STANDARD_DM)
-- -- --605,774
--
--
--
-- create or replace temp table non_dmable_exclusion as
-- select enterprise_customer_id as ec_id from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_party_account
-- where party_account_id in
-- (select * from EDWS_PROD.PROD_CMT_CAMPAIGN_01.SNP_JSG_NON_DMABLE);
--
-- create or replace temp table mailable_exclusion as
-- select ec_id
-- from EDWS_PROD.PROD_CMT_PRESENTATION.VW_CA_CUSTOMER_ACCOUNT
-- where mailable = '0';
--
--
--
--
-- select count(distinct ec_id) from mailable_exclusion
-- where ec_id in
--       (select * from non_dmable_exclusion )
-- --36,077,000
-- --45,609,782 mailable
--
-- select count(distinct EC_ID) from TD03_2223_STANDARD_DM where EC_ID not in
-- (select ec_id from non_dmable_exclusion);
-- -- 556820 dm exclusions
--
-- select count(distinct EC_ID) from TD03_2223_STANDARD_DM where EC_ID not in
-- (select ec_id from mailable_exclusion);
-- -- 274675 missing exclusion
--
-- select count(distinct ec_id) from
--                          TD03_2223_STANDARD_DM
-- where ec_id in (select * from mailable_exclusion)