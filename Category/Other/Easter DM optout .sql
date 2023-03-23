---------------------------------------------- CHECK OPT OUTS FOR EAST18 DM --------------------------------------------
-- UPDATED: Sidney Rogers 02.02.18
-- Mail File is now created after the Opt Outs have been sent so code changed to reflect this


select count(*), contactstatuscode from all_campaign_SH group by 2;limit 5; --19,681,092



-- SET ENVIRONMENT --
use role        RL_PROD_MARKETING_ANALYTICS;
use warehouse   ADW_2XLARGE_ADHOC_WH;
use database    CUSTOMER_ANALYTICS;
use schema      PRODUCT_REPORTING;

-- ASSIGN MACROS --
-- As per User Variable in Unica --
set CampaignName = 'XMS222';
set CampaignType = 'DM';
set CampaignCode = 'C000002629';
set FinYear	 = '2223';

create or replace temp table flowchart_SH as
select *
from "EDWS_PROD"."PROD_CMT_PRESENTATION"."VW_CHRH_CAMPAIGN_EC"
where CAMPAIGNCODE = $CampaignCode;

set FlowchartID = (select flowchartid from flowchart_SH);

-- GET ALL OF THE CAMPAIGN FOR THAT ID --
-- CHECK THE CONTACTSTATUSCODE IF IT HASN'T CHANGED TO DELIVERED THIS MEANS THEY WERE OPT-OUT BY ST IVES --
-- THE CONTROL WILL ALSO BE A CONTACTSTATUS CODE --
create or replace temp table all_campaign_SH as
select *
from "EDWS_PROD"."PROD_CMT_PRESENTATION"."VW_CH_FACT_EC"
where flowchartid = $FlowchartID;

create or replace temp table sr_id_SH as
    select EC_ID, SR_ID, HH_ID, FLOWCHARTID, TREATMENTINSTID, PACKAGEID, CONTACTDATETIME, UPDATEDATETIME,
           DATEID, CONTACTSTATUSCODE, CONTACTSTATUSDESC, RUNDATETIME, TREATMENTCODE, CREATEDATE, TIER_CODE,
           TRAFFIC_LIGHT, AVERAGE_SPEND, TEST_AND_LEARN, BUYER_TYPE, CATEGORY_SPEND_SEGMENTATION
    from (
        select h.*, row_number() over (partition by sr_id order by TREATMENTCODE ) as rno
        from all_campaign_SH h
        where contactstatuscode='CAMPAIGN_SEND')
where  rno = 1;
-- 946201
select count(*) from sr_id_SH;


select contactstatuscode,
       count(*)                                as Freq,
       count(*) / (select count(*) from sr_id_SH) as Prop
from sr_id_SH
group by 1;

create or replace temp table suppress_SH as
select a.sr_id,
       c.ADDRESSSUPPRESSIONFLAG,
       c.JSGROC_DM,
       c.QUINETICGASSUPPRFLAG
from "EDWS_PROD"."PROD_CMT_PRESENTATION"."VW_CA_CUSTOMER_ACCOUNT" as a
         left join "EDWS_PROD"."PROD_CMT_PRESENTATION"."VW_CA_SUMMARY" as b on a.sr_id = b.sr_id
         left join "EDWS_PROD"."PROD_CMT_PRESENTATION"."VW_EC_RT_SUPRESSION" as c on b.ec_id = c.ec_id
where c.JSGROC_DM not in ('Y', 'LI', 'LIN')
   or c.ADDRESSSUPPRESSIONFLAG = 'Y'
   or QUINETICGASSUPPRFLAG = 'Y'
order by a.sr_id;
select count(*) from suppress_SH;
-- 48038790

-- CREATE A COLUMN CONTAINING ROW NUMBER THIS WILL HELP IN THE NEXT STAGE WHEN REMOVING THE DUPLICATES
create or replace temp table suppress_SH as
    select *, row_number() over(order by sr_id) as rn
from suppress_SH;

-- REMOVE DUPLICATES
create or replace temp table suppress_SH as
select SR_ID, ADDRESSSUPPRESSIONFLAG, JSGROC_DM, QUINETICGASSUPPRFLAG
    from (select h.*, row_number() over (partition by sr_id order by rn) as rno
from suppress_SH h)
where  rno = 1;
--48,023,034

create or replace temp table deactivated_SH as
    select sr_id, LMS_STATUS_CODE
    from "EDWS_PROD"."PROD_CMT_PRESENTATION"."VW_CA_CUSTOMER_ACCOUNT"
    where LMS_STATUS_CODE in ('PD', -- permenantly deleted
			      'DE'); -- deactivated
select count(*) from deactivated_SH;
-- 27468029

create or replace temp table OptOuts_SH as
    select s.EC_ID, s.SR_ID, s.HH_ID, s.FLOWCHARTID, s.TREATMENTINSTID, s.PACKAGEID, s.CONTACTDATETIME, s.UPDATEDATETIME,
           s.DATEID, s.CONTACTSTATUSCODE, s.CONTACTSTATUSDESC, s.RUNDATETIME, s.TREATMENTCODE, s.CREATEDATE, s.TIER_CODE,
           s.TRAFFIC_LIGHT, s.AVERAGE_SPEND, s.TEST_AND_LEARN, s.BUYER_TYPE, s.CATEGORY_SPEND_SEGMENTATION, d.LMS_STATUS_CODE,
           su.ADDRESSSUPPRESSIONFLAG,su.JSGROC_DM, su.QUINETICGASSUPPRFLAG,
           case when trim(s.sr_id)=trim(d.sr_id) then 1 else 0 end as deac_exc,
           case when trim(s.sr_id)=trim(su.sr_id) then 1 else 0 end as mail_exc
                from sr_id_SH s
                     left join deactivated_SH d on trim(s.sr_id)=trim(d.sr_id)
                     left join suppress_SH su on trim(s.sr_id)=trim(su.sr_id);
select count(*) from OptOuts_SH;
-- 946201

create or replace temp table export1_SH as
	select distinct
	sr_id
	,deac_exc
	,mail_exc
	,JSGROC_DM
	,ADDRESSSUPPRESSIONFLAG
	,QUINETICGASSUPPRFLAG
	,flowchartid
	from OptOuts_SH
	where deac_exc = 1 or mail_exc = 1;
select count (*) from export1_SH;
-- 163784

create or replace temp table export1_1_SH as
	select sr_id, deac_exc, mail_exc,flowchartid,
		case when ADDRESSSUPPRESSIONFLAG = 'Y' then 1 end as address_suppression,
		case when JSGROC_DM not in ('Y','LI','LIN') then 1 end as js_groc_dm_gdpr,
		case when QUINETICGASSUPPRFLAG = 'Y' then 1 end as gone_away
	from export1_SH;
select count(*) from export1_1_SH;
--163784

select
	sum(deac_exc) as Deactivated,
	sum(address_suppression) as Address_Suppression,
	sum(JS_GROC_DM_GDPR) as GDPR_Permissions,
	sum(gone_away) as Gone_Aways
from export1_1_SH;

-- REMOVE DUPLICATES
create or replace temp table export1_1_SH as
select SR_ID, DEAC_EXC, MAIL_EXC, FLOWCHARTID, ADDRESS_SUPPRESSION, JS_GROC_DM_GDPR, GONE_AWAY
from (
         select h.*,
                row_number() over (partition by sr_id order by JS_GROC_DM_GDPR) as rno
         from export1_1_SH h)
where rno = 1;

select count(*) from export1_1_SH;
--163,784

-- SAVE IN PRODUCT REPORTING
set mytab2 = concat('CUSTOMER_ANALYTICS.PRODUCT_REPORTING.',$CampaignName,'_',$CampaignType,'_', $FinYear, '_OptOuts');
select $mytab2;

create or replace table identifier($mytab2) as
select * from export1_1_SH;

-- EXPORT USING EXPORT DATA ICON ON THE RIGHT AND SAVE IN OUTPUT FOLDER IN A RESPECTIVE DM CAMPAIGN. EXAMPLE BELOW --
-- \\feltfps0004\GENGRPSHARE0034\Campmana2\Change & Development\Cross-Cat Campaigns\LWFL\202021\LWFL 16\05. Opt outs --
select * from export1_1_SH;

select * from CUSTOMER_ANALYTICS.PRODUCT_REPORTING.EST223_DM_2324_OptOuts;
select count(*) from CUSTOMER_ANALYTICS.PRODUCT_REPORTING.XMS222_DM_2223_OptOuts;

/*

---------- THIS PART IS TO CREATE THE RETURN FILE FROM PARAGON - NOW AFTER THE OPT OUTS HAVE BEEN PROVIDED -------------

create or replace temp table mailed_cust as
select  distinct(sr_id)
		, EC_ID
		, contactstatuscode as STATUS
from all_campaign
where contactstatuscode in ('DELIVERED');

-- WE CAN SAVE TO PRODUCT REPORTING --
set mytab = concat('CUSTOMER_ANALYTICS.PRODUCT_REPORTING.',$CampaignName,'_', $CampaignType,'_',$FinYear, '_mailed');
select $mytab;

create or replace table identifier($mytab) as
    select * from mailed_cust order by sr_id;

*/
