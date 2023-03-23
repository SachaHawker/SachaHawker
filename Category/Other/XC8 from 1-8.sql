/**********************************************************************************************************************
Project:        XC - PCA Code
Date:           01 July 2020

Background:
This is the main BAU post campaign analysis code for cross category, direct mail and pp selections. This can be run
during and after campaigns are live. The aim of this code is to gather campaign results and other key KPIs.

Change History:
Name:           Date:           Note:
Yunus Malik     01JUL2020       Setup repository and project. Rewriting in SQL and making code more efficient.
Yunus Malik     16SEP2020       Change dynamic code to sit inside a stored procedure.
**********************************************************************************************************************/

/**********************************************************************************************************************
* 01 Variables & Selection data - This script sets all the variables for the PCA. It then grabs the campaign data
                                  from contact history (this creates the CH Selection File). In the final step
                                  an online customer file is replicated from the CH Selection File, selecting their
                                  online SR_IDs and choosing only product level offers. Customer can have multiple
                                  online accounts.

* Important Tables - CH_SELECTION_FILE_1
                     OL_SELECTION_FILE_1
                     CUSTOMER_FILE_ALL_[file_abbrev]
**********************************************************************************************************************/

use role RL_PROD_MARKETING_ANALYTICS;
use database CUSTOMER_ANALYTICS;
use warehouse WHS_PROD_MARKETING_ANALYTICS_LARGE;
use schema PRODUCT_REPORTING;

/**********************************************************************************************************************
* Defining variables **************************************************************************************************
**********************************************************************************************************************/

set initials = 'SH';
set test_group = 'N';
-- if test_group is Y fill in the tbl info below, otherwise leave blank ''
set tblvar = 'segment_variant'; -- column name of segment or test variable
set tblloc = 'segment_table'; -- location of segment or test table to join to

/*
create or replace temp table OL_tbl as
    select b.sr_id
        , case when sku1 <1000 then 'variant_1' else 'variant_2' end as segment_variant
    from customer_analytics.SANDBOX.IA_xc33202_final_sel a
    inner join EDWS_PROD.PROD_CMT_PRESENTATION.VW_CA_CUSTOMER_ACCOUNT b
        on a.full_nectar_card_num = b.full_nectar_card_num
    where b.party_account_type_code ='02'
    order by sr_id;

create or replace temp table segment_table as
select sr_id, case when sku1 <=1000 then 'variant_1' else 'variant_2' end as segment_variant
from customer_analytics.SANDBOX.IA_xc33202_final_sel
union all
select * from OL_tbl

*/

-- to get selection file from CHRH
set camp_type = 'CT';
set camp_code = 'C000002611';
set fc_name = '2223_XCAT_PII';
-- campaign specific variables
set file_abbrev = 'X08222';
-- campaign dates - update these dates
set camp_start_date = '2022-12-07';
set print_stop_date = '2022-12-27';
set camp_end_date = '2023-01-17';




set pre_period_date = to_char(to_date($camp_start_date) - 84);
select $camp_start_date, $print_stop_date, $camp_end_date, $pre_period_date;

-- list thresholds you are using here, if you want to customise PPT3
set counter_thresholds =
    '[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,235,236,237,238,239,240,241,242,243,244,245,246,247,248,249]';

set custom_thresholds = '[]';
set custom_thresh_name = '';

-- additional costs and metrics
set print_cost = 0.0133;
set nectarpoint_cost = 0.00425;
set valassis_cost = 0.0142;
set halo_margin = 0.216;--0.2239;
set ol_halo_margin = 0.089;
/* If CT then set equal to 0, if DM then set dm_cost equal to total cost of the DM campaign */
set dm_cost = 360000;

-- the same automated flowchart is run each selection so we have to select the correct run for the pca
-- cannot just select max(runid) as this could select a campaign that has been run but is not yet live
-- run the code below and select copy the runid to the uservar below, check the rundate and volumes to ensure you're selecting the correct run!
    select runid, rundatetime, count(distinct sr_id)
    from EDWS_PROD.PROD_CMT_PRESENTATION.VW_CH_FACT_EC a inner join EDWS_PROD.PROD_CMT_PRESENTATION.VW_CHRH_CAMPAIGN_EC b on a.flowchartid = b.flowchartid
    inner join EDWS_PROD.PROD_CMT_PRESENTATION.VW_CH_OFFER_ATTRIBUTE_EC c on a.treatmentcode = c.treatmentcode
    where campaigncode = $camp_code and flowchartname = $fc_name
        and contactstatuscode <> 'SUPPRESSED' group by 1,2 order by 2;

-- update runid using the code above, ensure you have selected the correct runid
set runid = 99673;
select $runid;

show variables;

-- perm tables in PCA
set cust_tbl_name = 'CUSTOMER_FILE_ALL'||'_'||$file_abbrev||'_'||$initials; -- script 01
-- includes all customers (instore + online) and their offers
set QSF_tbl_name = 'QUALIFYING_SKUS_FINAL'||'_'||$file_abbrev||'_'||$initials; -- script 03
-- includes all product and threshold relates skus (skus that can be redeemed against a threshold coupon)

/**********************************************************************************************************************
* Selection File ******************************************************************************************************
**********************************************************************************************************************/

-- pulling contacted and control customers from CH
create or replace temp table CH_Customer_File as
    select distinct ec_id,
                    sr_id,
                    contactstatuscode,
                    a.treatmentcode,
                    rundatetime

    from EDWS_PROD.PROD_CMT_PRESENTATION.VW_CH_FACT_EC a
    inner join EDWS_PROD.PROD_CMT_PRESENTATION.VW_CHRH_CAMPAIGN_EC b
        on a.flowchartid = b.flowchartid
        inner join EDWS_PROD.PROD_CMT_PRESENTATION.VW_CH_OFFER_ATTRIBUTE_EC c
        on a.treatmentcode = c.treatmentcode
    where campaigncode = $camp_code and flowchartname = $fc_name
        and contactstatuscode <> 'SUPPRESSED'
and c.runid=$runid;

select * from CH_Customer_File;

-- select count(*),
--        count(ec_id),
--        count(distinct sr_id),
--        rundatetime
-- from CH_Customer_File
-- group by rundatetime;




-- joining the customer file to get offers
create or replace temp table CH_Selection_File as
    select a.*,
           b.offerid,
           b.controlflag,
           b.promoted_skus,
           b.barcode,
           b.targeted_skus,
           b.reward_value_fixed,
           b.reward_value_money,
           runid
    from CH_Customer_File a
        inner join EDWS_PROD.PROD_CMT_PRESENTATION.VW_CH_OFFER_ATTRIBUTE_EC b
            on a.treatmentcode = b.treatmentcode
--     where runid=$runid
order by sr_id;

select count(*),
       count(ec_id),
       count(distinct sr_id),
       rundatetime,
       runid
from CH_Selection_File
group by 4,5
order by 4;
-- 20399085,20399085,4627845,2022-10-14 15:51:22.000000,98284
-- 20399085,20399085,4627845,2022-10-18 08:45:21.000000,98352
-- 20399085,20399085,4627845,2022-10-20 12:25:14.000000,98453
-- 24553336,24553336,4669798,2022-11-25 09:03:00.000000,99673

--joining to coupon details to get coupon id
create or replace temp table CH_Selection_File as
    select a.*,
           b.coupon_id as couponid
    from CH_Selection_File a
        inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_COUPON_DETAILS b
            on a.barcode=b.barcode;

select count(distinct sr_id), contactstatuscode, controlflag from CH_Selection_File group by 2,3;
select count(*) from CH_Selection_File;
-- contains every customers offer per row (not a distinct table)

-- using selection file I made with only 8 offers
create or replace temp table CH_SELECTION_FILE as
select * from CH_SELECTION_FILE_1_8;

/**********************************************************************************************************************
* Test Groups *********************************************************************************************************
**********************************************************************************************************************/

-- IF Y then add segment to table else use standard selection table
create or replace procedure add_seg(POP_INPUT string, VAR string, TBL string, OTBL string, NTBL string)
returns string
language javascript
strict
as
$$
// create first table
var sqlcommand1 =
"create or replace temp table " + NTBL + " as\
    select a.*,\
           b." + VAR + " as pop\
    from " + OTBL + " a\
    left join " + TBL + " b\
        on a.sr_id = b.sr_id;";

var stmt1 = snowflake.createStatement({sqlText: sqlcommand1 })

// create second table
var sqlcommand2 =
"create or replace temp table " + NTBL + " as\
    select *,\
           'All' as pop\
    from " + OTBL + ";";

var stmt2 = snowflake.createStatement({sqlText: sqlcommand2 })

if (POP_INPUT == 'Y') {
stmt1.execute();
}
else {
stmt2.execute();
}
 return '💰';
$$;

call add_seg($test_group, $tblvar, $tblloc, 'CH_Selection_File', 'CH_Selection_File_1');

select * from CH_Selection_File_1;
select count(*) from CH_Selection_File_1;

/**********************************************************************************************************************
* Online Customers ****************************************************************************************************
**********************************************************************************************************************/

-- Create replica CH Selection File for our online customers
-- Product level skus only: sku >= 1000 or gol = 'Yes'

create or replace temp table OL_Selection_File as
    select a.*,
           b.full_nectar_card_num

    from CH_Selection_File_1 a
    inner join EDWS_PROD.PROD_CMT_PRESENTATION.VW_CA_CUSTOMER_ACCOUNT b
        on a.sr_id = b.sr_id
    inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_COUPON_DETAILS c
        on a.couponid = c.COUPON_ID
    where c.gol = 'Yes'
    order by sr_id;

select count(*) from OL_Selection_File;
-- 13494724

create or replace temp table OL_Selection_File_1 as
    select a.*,
           b.sr_id as ol_sr_id

    from OL_Selection_File a
    inner join EDWS_PROD.PROD_CMT_PRESENTATION.VW_CA_CUSTOMER_ACCOUNT b
        on a.full_nectar_card_num = b.full_nectar_card_num
    where b.party_account_type_code ='02'
    order by sr_id;

select count(*) from OL_Selection_File_1;
-- 5776323

-- Since customers can have multiple online accounts, offers get uploaded to all that are linked
-- Join CH customer file to OL customer file
create or replace temp table OL_IS_CUSTOMER_FILE as
    select ec_id,
           sr_id,
           '04' as party_account_type_code,
           treatmentcode,
           offerid,
           controlflag,
           targeted_skus as SKU,
           barcode,
           couponid,
           reward_value_fixed,
           reward_value_money,
           pop

    from CH_Selection_File_1
    union
    select ec_id,
           ol_sr_id as sr_id,
           '02' as party_account_type_code,
           treatmentcode,
           offerid,
           controlflag,
           targeted_skus as SKU,
           barcode,
           couponid,
           reward_value_fixed,
           reward_value_money,
           pop

    from OL_Selection_File_1;

select count(*) from OL_IS_CUSTOMER_FILE;
select * from OL_IS_CUSTOMER_FILE;
-- 30695876 - will change over time

/**********************************************************************************************************************
* Online Coupon ID ****************************************************************************************************
**********************************************************************************************************************/

-- joining to coupon details table to get OC_IDs
create or replace table identifier($cust_tbl_name) as
    select a.*,
           b.oc_id,
           b.gol

    from OL_IS_CUSTOMER_FILE a
    left join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_COUPON_DETAILS b
        on substr(cast(a.barcode as string),1,14) = substr(cast(b.barcode as string),1,14)
        -- and a.couponid = cast(b.coupon_id as string)
    where b.status = 'ACTIVE'; -- please check this flag and check the distinct barcodes/offers

select * from identifier($cust_tbl_name);
select count(oc_id), count(*) from identifier($cust_tbl_name);
-- 30737496

set target_custs = (select count(distinct sr_id) from CH_Selection_File where controlflag = 'TARGET');
select $target_custs

-- The final table of this script is saved in the SANDBOX, so you can reference it from there if your session ends.

/**********************************************************************************************************************
***********************************************************************************************************************
**********************************************************************************************************************/
