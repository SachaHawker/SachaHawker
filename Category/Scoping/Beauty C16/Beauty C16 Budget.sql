/***********************************************************************************************************************
CREATED BY CLARA COKRO 19/08/19
CODE FORMATS THE CROSS CAT CUSTOMER SELECTION FROM SAS PRE CONTROL GROUP
IT WILL ESTIMATE ALL THE METRICS AND SEND THE FINAL FILE TO YOURSELF
***********************************************************************************************************************/

/***********************************************************************************************************************
ADJUSTED BY IMOGEN STEELE 27/02/2020
CHANGES TO CALCULATE BUDGET ASSUMPTIONS FROM PCA REPOSITORY AND TO DIFFERENTIATE BETWEEN PRODUCT AND THRESHOLD OFFERS
***********************************************************************************************************************/

/***********************************************************************************************************************
ADDED TO SNOWFLAKE BY IQQRA AZIZ ON 31/07/2020
***********************************************************************************************************************/

use role RL_PROD_MARKETING_ANALYTICS;
use warehouse WHS_PROD_MARKETING_ANALYTICS_X2LARGE;
use database CUSTOMER_ANALYTICS;
use schema PRODUCT_REPORTING;

set CampaignName    = 'BC16CT';         -- As per User Variable in Unica --
set CampaignType    = 'CT';
set username        = 'SH';
set control_grp     = 0.1 ;            -- percentage of control group, normally 0.08 or 0.1 --
set print_dist      = 0.60;             -- estimated print distribution --

set tab             = concat('CUSTOMER_ANALYTICS.PRODUCT_REPORTING.AJ_X07222_FINAL_SEL');
select $tab;
-- ESTIMATED REDEMPTION RATE AND SCR FOR ANY CELL THAT DOESNT SIT WITHIN THE STANDARD CATEGORY AREA MANAGER --
set other_RR    = 0.05;
set other_SCR   = 2.5;

-- VARIABLES FOR BUDGET REPOSITORIES
set print_start = '2023-03-01';
set print_stop  = '2023-03-22';
set final_red   = '2023-04-11';

set year = (select year(to_date($print_start)));
set fin_yr = (select case when $print_start < '2020-03-08' then '1920'
                          when $print_start < '2021-03-07' then '2021'
                          when $print_start >= '2021-03-07' then '2122'
                          when $print_start >= '2022-03-06' then '2223'
                     else '9999' end as Fin_Year);


-- pull margin & asp for the two beauty subcats
create or replace temp table produce_base as
select distinct a.sku
                ,a.SKU_DESC
                ,manager_resp
                ,avg(case when b.avg_selling_price_12weeks>0 then b.avg_selling_price_12weeks else null end) as asp
                ,avg(case when b.margin_12weeks>0 then b.margin_12weeks else null end) as margin
    from EDWS_PROD.PROD_CMT_PRESENTATION.VW_SKU a
    inner join EDWS_PROD.PROD_CMT_PRESENTATION.VW_SKU_SUMMARY b
        on a.sku_key = b.sku_key
    where a.end_date is null
    and a.sub_category in (832, 868, 19,
30,
76,
329,
330,
331,
332,
337,
338,
340,
342,
348,
360,
365,
367,
369,
381,
382,
383,
396,
496,
609,
666,
700,
729,
832,
861,
868)
group by 1,2,3
order by 4;

select count(distinct sku) from produce_base;

-- get transaction info
 create or replace temp table produce_trx as
    select   stl.party_account_id,
           stl.transaction_date,
           stl.transaction_time,
           stl.party_account_type_code,
           stl.location_key,
           stl.till_number,
           lm.location
			, sum(stl.extended_price) as sales
            , sum(stl.item_quantity) as quantity
			, avg(margin) as avg_margin
	from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_shopping_transaction_line as stl

	inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_ean as ean
		on stl.ean_key = ean.ean_key

	inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_c_sku_map as sku
		on ean.sku_key = sku.sku_key

	inner join produce_base as san on sku.sku = san.sku
    inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.vw_c_location_map as lm
        on  lm.location_key = stl.location_key
        and lm.js_restaurant_ind = 'N' /* no restaurants */
        where transaction_date > dateadd(day,-7*26,'2023-02-10')
        and transaction_date <= '2023-02-10'
    	and stl.extended_price>0 /*transaction value bigger than zero*/
		and stl.party_account_type_code in ('04') /*in store only for threshold*/
	group by 1,2,3,4,5,6,7;

select percentile_cont(0.25) within group (order by sales)  as avg_spend_p25
     , percentile_cont(0.5) within group (order by sales)  as avg_spend_p50
     , percentile_cont(0.75) within group (order by sales)  as avg_spend_p75
     , avg(avg_margin) as avg_margin
from produce_trx;

-- 4.00000	6.00000	10.00000	0.366707512182



-- Beauty selection was done entirely in unica
-- pulling targeted customers from CH
create or replace temp table CH_Customer_File as
    select distinct ec_id,
                    sr_id,
                    HH_ID,
                    contactstatuscode,
                    treatmentcode

    from EDWS_PROD.PROD_CMT_PRESENTATION.VW_CH_FACT_EC a
    inner join EDWS_PROD.PROD_CMT_PRESENTATION.VW_CHRH_CAMPAIGN_EC b
        on a.flowchartid = b.flowchartid
    where campaigncode = 'C000002698' and flowchartname = 'Beauty_C16_CaT'
        and contactstatuscode <> 'SUPPRESSED';

-- joining the customer file to get offers
create or replace temp table CH_Selection_File as
    select a.*,
           b.offerid,
           b.controlflag,
           b.promoted_skus,
           b.barcode,
           b.couponid,
           b.targeted_skus,
           b.reward_value_fixed,
           b.reward_value_money

    from CH_Customer_File a
    inner join EDWS_PROD.PROD_CMT_PRESENTATION.VW_CH_OFFER_ATTRIBUTE_EC b
        on a.treatmentcode = b.treatmentcode
    order by sr_id;

-- should be one coupon per customer, check
select count(*), count(distinct sr_id) from CH_Selection_File
-- 1970953	1970953

Select * from CH_SELECTION_FILE;

-- Target counts
select barcode, reward_value_money, reward_value_fixed, count(distinct sr_id) from CH_Selection_File where controlflag='TARGET'
group by 1,2,3;



-- IMPORT THE SELECTION FILE YOU WANT FROM CROSSCAT LIBRARY (FOR MIGRATION PURPOSES AM ONLY IMPORTING 500,000)
-- ZEROIFNULL IS APPLIED AS WHEN TRANSPOSING IF CELL IS NULL THEN ROW IS NOT ADDED TO TRANSPOSED DATA
-- RENAMING THE POINTS COLUMN AS THEY CAN'T BE SELECTED IN PIVOT FUNCTION LATER ON
-- AJ: recreating fields needed from CH
create or replace temp table sorted as
select SR_ID, EC_ID, HH_ID
     , 0 as SKU1
     , zeroifnull(reward_value_fixed) as BP1
--           , zeroifnull(substr(reward_value_money,2,length(reward_value_money))) as BPo1
     , zeroifnull(reward_value_money) as BPo1
     , BARCODE as BARCODE1
from CH_Selection_File a
order by sr_id;

-- TRANSPOSES THE POINTS VALUE FOR THE SKU IN THE SELECTION FILE --
-- CODE ADDS A COUNT TO EACH CUSTOMER FOR THEIR SELECTED SKUS, SO THAT WE CAN USE THIS AND SR_ID AS AN IDENTIFIER LATER ON --
create or replace temp table trans_points as
    select sr_id, points,
           case when counts = 'BP1' then 1
               end as count

    from sorted
    unpivot(points for counts in (BP1))
    order by sr_id, count;

-- TRANSPOSES THE SKUS IN THE SELECTION FILE AND KEEPS NECESSARY FIELDS --
-- CODE ADDS A COUNT TO EACH CUSTOMER FOR THEIR SELECTED SKUS, SO THAT WE CAN USE THIS AND SR_ID AS AN IDENTIFIER LATER ON --
create or replace temp table trans_sku as
    select sr_id, skus,
           case when counts = 'SKU1' then 1
               end as count

    from sorted
    unpivot(skus for counts in (sku1))
    order by sr_id, count;

create or replace temp table trans_pounds as
    select sr_id, pounds,
           case when counts = 'BPO1' then 1
               end as count

    from sorted
    unpivot(pounds for counts in (BPo1))
    order by sr_id, count;


select count(distinct skus) from trans_sku;

-- MERGES THE POINTS AND SKU DATASETS ON SR_ID AND COUNT SO THE POINTS AND SKU MATCH --
create or replace temp table merged as
         select s.SR_ID, s.count, s.skus, p.points, tp.pounds, case when tp.pounds>0 then 'POUNDS' else 'POINTS' end as offer_type
         from trans_sku s
         inner join trans_points p on s.sr_id=p.sr_id and s.count=p.count
         inner join trans_pounds tp on p.sr_id=tp.sr_id and  p.count = tp.count;

select offer_type, count(*) from merged group by 1;


create or replace temp table addcountermanager as
         select sr_id, 1 as sku_rank, 0 as sku, points, pounds, offer_type,

         case when sku=0 then 0.36 end as margin,

         case when sku=0 then 'CM Health & Beauty' end as manager_resp,

        case when sku=0 then 4.5 end as asp
    from merged;


------------------------------------------------------------------------------------------------------------------------
---- CODE FROM HERE IS ORGINAL BUDGET CODE. THIS CREATES RAW DATA TO USE FOR THE BUDGETS BASED ON THE ABOVE DATASET ----
------- IMOGEN (FEB 2020) - ADDED IN FLAG FOR THRESHOLDS FOR USE IN BUDGET ASSUMPTION CALCULATIONS FURTHER DOWN --------
------------------------------------------------------------------------------------------------------------------------

-- create one column for all offer value

create or replace temp table addcountermanager as
    select sr_id, sku_rank, sku, offer_type, case when offer_type='POINTS' then points else pounds end as offer_value, margin, manager_resp, asp
    from addcountermanager;

select * from addcountermanager limit 100;


------------------------------------------------------------------------------------------------------------------------
----------------------------------------- AGGREGATE CUSTOMER DATA FOR BUDGET -------------------------------------------
------------------------------------------------------------------------------------------------------------------------
set Cust_Group = 'BAU';

set mytab1 = concat('prod_', $Cust_Group, '_1');
create or replace temp table identifier($mytab1) as
	select distinct	a.sku,
			a.Manager_Resp as category,
		    a.asp,
			a.margin,
	        a.offer_type,
	        a.offer_value,
			b.targets,
			(case when a.sku<1227 then 1 else 0 end) as Threshold
	from
		addcountermanager a
	left join
		(select sku, offer_type, offer_value, count(distinct sr_id) as targets
		from addcountermanager
		group by 1, 2,3) as b
		on a.sku = b.sku and
		a.offer_value = b.offer_value and a.offer_type=b.offer_type
order by 1;


set mytab2 = concat('Cust_Count_', $Cust_Group);
create or replace temp table identifier($mytab2) as
	select count (distinct sr_id) as Customers
	from addcountermanager;
------------------------------------------------------------------------------------------------------------------------
--------------------- PULLS AVERAGE REDEMPTION RATES AND JS_SCR FROM PREVIOUS PCAs IN THE REPOSITORY -------------------
------------------------------- (GROUPED BY MANAGER RESP AND IF THRESHOLD OR PRODUCT LEVEL) ----------------------------

-- RANKING THE REPO BY MOST RECENT CAMPAIGNS (EXCLUDING ANY LIVE CAMPAIGNS)
create or replace temp table ranked_pca_repo as
select campaign_code, camp_type, Fin_Year, manager_resp, THRESHOLD, print_start, FINAL_REDEMPTION,
       prints, reds, reds/prints as red_rate, trad_scr,
       dense_rank() OVER(partition by dummy ORDER BY PRINT_START desc,campaign_code DESC) AS PowerRank
from (
select 1 as dummy,
       campaign_code,
       camp_type,
       case
           when print_start < '2020-03-08' then '1920'
           when print_start < '2021-03-07' then '2021'
           when print_start >= '2021-03-07' then '2122'
           when print_start >= '2022-03-06' then '2223'
           else '9999' end                        as Fin_Year,
       manager_resp,
       THRESHOLD,
       print_start,
       FINAL_REDEMPTION,
       sum(prints)                                as prints,
       sum(REDEMPTION)                            as reds,
       (sum(Sales_Uplift) / sum(redemption_cost)) as trad_scr
from PRODUCT_XCAT_PCA_REPOSITORY
where CAMP_TYPE ='CT'                   -- on coupon at till campaign results
  and manager_resp is not null          -- removing null manager_resp
  and prints > 10000                    -- removing low prints (results will not be accurate)
  and CAMPAIGN_CODE not ilike '%PP%'    -- removing any PP campaigns
  and FINAL_REDEMPTION < current_date() -- only picking out completed campaigns
group by 1, 2, 3, 4, 5, 6, 7, 8)
where prints <> 77425                   -- removing duplicated results
;

-- CREATING AN OUTLIER TABLE. THIS WILL PUT TOGETHER A TABLE WHICH WILL DETERMINE THE THRESHOLD FOR OUTLIERS
        -- if RR range for an offer is greater than 0.06, then we will overwrite that offer RR with either a low_RR_outliers or high_RR_outliers
        -- if SCR range for an offer is greater than 3.5, then we will overwrite that offer SCR with either a low_SCR_outliers or high_SCR_outliers
create or replace temp table outlier_table as
select manager_resp,
       Threshold,
       -- LOGIC TO DECIDE WHETHER WE WILL USE THE RR/SCR FROM HISTORICAL OFFER PERFORMANCE
       case when max(red_rate) - min(red_rate) >= 0.06 then 1 else 0 end as overwrite_rr,
       case when max(trad_scr) - min(trad_scr) >= 3.5 then 1 else 0 end  as overwrite_scr,
       -- CALCULATING Q1 AND Q3
       percentile_cont(0.25) within group (order by trad_scr)            as scr_p25,
       percentile_cont(0.75) within group (order by trad_scr)            as scr_p75,
       percentile_cont(0.25) within group (order by RED_RATE)            as rr_p25,
       percentile_cont(0.75) within group (order by RED_RATE)            as rr_p75,
       -- APPLYING LOGIC IF WE WILL NOT BE USING THE RR/SCR FROM HISTORICAL OFFER PERFORMANCE TO CREATE DUMMY VALUES TO USE
       scr_p25 - (0.5 * (scr_p75 - scr_p25))                             as low_scr_outliers,
       scr_p75 + (0.5 * (scr_p75 - scr_p25))                             as high_scr_outliers,
       rr_p25 - (0.5 * (rr_p75 - rr_p25))                                as low_rr_outliers,
       rr_p75 + (0.25 * (rr_p75 - rr_p25))                               as high_rr_outliers
from ranked_pca_repo
group by 1, 2;

-- USE OUTLIER TABLE TO REPLACE THE OUTLIERS IN OUR RESULTS TABLE WITH MORE REASONABLE RESULTS.
create or replace temp table prev_kpi_cle as
select a.CAMPAIGN_CODE
     , a.CAMP_TYPE
     , a.FIN_YEAR
     , a.MANAGER_RESP
     , a.THRESHOLD
     , a.PRINT_START
     , a.FINAL_REDEMPTION
     , a.PRINTS
     , a.REDS
     , a.RED_RATE
     , case
           when a.RED_RATE < LOW_RR_OUTLIERS and overwrite_rr = 1 then LOW_RR_OUTLIERS
           when a.RED_RATE > HIGH_RR_OUTLIERS and overwrite_rr = 1 then HIGH_RR_OUTLIERS
           else a.RED_RATE end as clean_rr
     , TRAD_SCR
     , case
           when a.TRAD_SCR < LOW_SCR_OUTLIERS and overwrite_scr = 1 then LOW_SCR_OUTLIERS
           when a.TRAD_SCR > HIGH_SCR_OUTLIERS and overwrite_scr = 1 then HIGH_SCR_OUTLIERS
           else a.TRAD_SCR end as clean_scr
     , PowerRank
from ranked_pca_repo a
         inner join outlier_table b on a.MANAGER_RESP = b.MANAGER_RESP and a.THRESHOLD = b.Threshold;

-- WEIGH MOST RECENT 3 CAMPAIGNS HIGHER THAN OTHERS
-- ASSIGN HIGHER WEIGHTS TO MORE RECENT CAMPAIGNS AND SMALLER WEIGHTS TO OLDER RESULTS
create or replace temp table Weights as
select manager_resp,
       threshold,
       count(*)                                                 as Freq,
       count_if(PowerRank <= 3)                                 as PR_Freq,
       iff(PR_Freq = Freq, 1 / PR_Freq, 0.25)                   as PR1,
       Freq - PR_Freq                                           as Other_Freq,
       iff(PR_Freq = Freq, 0, (1 - PR1 * PR_Freq) / Other_Freq) as Other_Weight
from prev_kpi_cle
group by 1, 2;

create or replace temp table weighted_kpis as
select a.*
     , case when PowerRank in (1, 2, 3) then b.PR1 else b.Other_Weight end as weight
     , clean_rr * weight                                                   as weighted_rr
     , clean_scr * weight                                                  as weighted_scr
from prev_kpi_cle a
         left join Weights b on a.MANAGER_RESP = b.manager_resp and a.THRESHOLD = b.threshold;

-- CHECK TO SEE ALL WEIGHTS FOR MANAGER RESP AND THRESHOLD COMBINATIONS EQUAL TO 1
select manager_resp, threshold,
       sum(weight) as Freq from weighted_kpis group by 1,2;

-- CREATING AGGREGATED RESULTS FOR EACH TYPE OF OFFER
create or replace temp table budget_assumption as
select distinct MANAGER_RESP
              , THRESHOLD
              , sum(weighted_rr) /sum(weight) as avg_rr
              , sum(weighted_scr)/sum(weight) as avg_scr
from weighted_kpis a
group by MANAGER_RESP, THRESHOLD
order by MANAGER_RESP, THRESHOLD;

------------------------------------------------------------------------------------------------------------------------

-- CALCULATE ALL THE METRICS AT SKU LEVEL --
create or replace temp table BAU_SKU as
select sku,
       category,
       ASP,
       margin,
       offer_type,
       case when offer_type = 'POINTS' then offer_value * 0.00425 else offer_value end as coupon_value,
       coalesce(avg_rr, $other_RR)                                 as est_RR,
       round(targets * (1 - $control_grp), 1)                      as target,
       round(target * $print_dist, 1)                              as distribution,
       round(distribution * est_rr, 1)                             as redemptions,
       redemptions * coupon_value                                  as red_cost,
       red_cost + (redemptions * 0.01421)                          as red_cost_with_valassis,
       (0.0133 * distribution)                                     as print_cost,
       coalesce(Avg_scr, $other_SCR)                               as trading_scr,
       red_cost * trading_scr                                      as target_sales_uplift,
       target_sales_uplift / (red_cost_with_valassis + print_cost) as JS_SCR,
       (target_sales_uplift * margin) - red_cost                   as trading_profit,
       trading_profit / red_cost                                   as trading_roi,
       ((target_sales_uplift * margin) - (red_cost_with_valassis + print_cost)) /
       (red_cost_with_valassis + print_cost)                       as JS_ROI
from prod_BAU_1 a
         left join BUDGET_ASSUMPTION b on a.category = b.manager_resp and a.threshold = b.threshold
order by sku;

select * from CUSTOMER_ANALYTICS.PRODUCT_REPORTING.prod_bau_1;

-- ESTIMATED NUMBER OF CUSTOMERS, MINUS CONTROL GROUP --
create or replace temp table cust as
select *, round(customers*(1-$control_grp),1) as customers_exl_control, 1 as num
from  CUST_COUNT_BAU;

set customers_exl_control = (select customers_exl_control from cust);


-- CALCULATE OVERALL BUDGET --
create or replace temp table Overall_budget as
select sum(redemptions)                                       as redemptions,
       sum(distribution)                                      as prints,
       sum(red_cost)                                          as red_cost,
       sum(case when offer_type = 'POINTS' then red_cost end) as red_cost_points,
       sum(case when offer_type = 'POUNDS' then red_cost end) as red_cost_pounds,
       sum(red_cost_with_valassis)                            as red_cost_with_valassis,
       sum(print_cost)                                        as print_cost,
       sum(distribution * margin) / sum(distribution)         as margin,
       sum(target_sales_uplift)                               as target_sales_uplift,
       sum(trading_profit)                                    as trading_profit
from BAU_SKU;



create or replace temp table Overall_budget as
select *,
       target_sales_uplift / red_cost                              as trading_scr,
       target_sales_uplift / (red_cost_with_valassis + print_cost) as js_scr,
       red_cost_with_valassis + print_cost                         as total_costs,
       (target_sales_uplift * margin) - total_costs                as js_profit,
       trading_profit / red_cost                                   as trading_roi,
       ((target_sales_uplift * margin) - (red_cost_with_valassis + print_cost)) /
       (red_cost_with_valassis + print_cost)                       as JS_ROI,
       1                                                           as num
from Overall_budget;


-- FINAL BUDGET WITH CUSTOMER NUMBER --
create or replace temp table Overall_budget2 as
select b.REDEMPTIONS / b.PRINTS as Red_rate, a.CUSTOMERS, a.CUSTOMERS_EXL_CONTROL, b.PRINTS,
       b.REDEMPTIONS, b.RED_COST, b.red_cost_points, b.red_cost_pounds, b.RED_COST_WITH_VALASSIS, b.PRINT_COST, b.TRADING_SCR,
       b.JS_SCR, b.TARGET_SALES_UPLIFT, b.TRADING_PROFIT, b.TOTAL_COSTS, b.JS_PROFIT, b.TRADING_ROI,
       b.JS_ROI, b.MARGIN
from cust a
         inner join Overall_budget b
         on a.num = b.num;


----------------------------------------------- CREATE PHASED BUDGET ---------------------------------------------------
create or replace temp table distribution
(
    week   number(10, 0),
    print  number(10, 2),
    red    number(10, 2),
    uplift number(10, 2)
);

-- REVISIT THIS AT THE END OF EACH FINANCIAL YEAR
insert into distribution
values (1, 0.50, 0.05, 0.05),
       (2, 0.86, 0.34, 0.34),
       (3, 0.98, 0.64, 0.62),
       (4, 1, 0.87, 0.87),
       (5, 1, 0.97, 0.98),
       (6, 1, 1, 1);

select * from distribution;

create or replace temp table total_all as
select 1 as week, a.* from Overall_budget2 a
union
select 2 as week, a.* from Overall_budget2 a
union
select 3 as week, a.* from Overall_budget2 a
union
select 4 as week, a.* from Overall_budget2 a
union
select 5 as week, a.* from Overall_budget2 a
union
select 6 as week, a.* from Overall_budget2 a;

select * from total_all;

-- USING THE PHASED BUDGET CREATED ABOVE AND OUR OVERALL LEVEL INFO WE CREATE OUR PHASED BUDGET
-- AND CALCULATE THE REST OF THE KPIS
create or replace temp table Phased_Budget_Final as
select a.week,
       round((prints * print), 0)                     as printsS,
       print_cost * print                             as print_costS,
       round((redemptions * red), 0)                  as redemptionsS,
       red_cost * red                                 as red_costS,
       red_cost_points * red                          as red_cost_points,
       red_cost_pounds * red                          as red_cost_pounds,
       red_cost_with_valassis * red                   as red_cost_with_valassisS,
       target_sales_uplift * uplift                   as target_sales_upliftS,
       trading_profit * uplift                        as trading_profitS,
       red_cost_with_valassisS + print_costS          as total_costSS,
       trading_profitS / red_costS                    as trading_roiS,
       (target_sales_upliftS * margin) - total_costSS as JS_profitS,
       JS_profitS / total_costSS                      as js_roiS,
       redemptionsS / printsS                         as redemption_rateS,
       total_costSS / redemptionsS                    as cost_per_redemptionS,
       target_sales_upliftS / redemptionsS            as uplift_per_redemptionS
from total_all a
         left join distribution b
                   on a.week = b.week;

-- renaming columns so they are consistent  with XCAT repository--
create or replace temp table phased_budget as
select week,
       printsS                 as prints,
       print_costS             as print_cost,
       redemptionsS            as redemptions,
       red_cost_points,
       red_cost_pounds,
       red_costS               as red_cost,
       red_cost_with_valassisS as red_cost_with_valassis,
       target_sales_upliftS    as target_sales_uplift,
       trading_profitS         as trading_profit,
       total_costSS            as total_cost,
       trading_roiS            as trading_roi,
       JS_profitS              as JS_profit,
       js_roiS                 as js_roi,
       redemption_rateS        as redemption_rate,
       cost_per_redemptionS    as cost_per_redemption,
       uplift_per_redemptionS  as uplift_per_redemption
from Phased_Budget_Final;

-- EXPORT FILES TO EXCEL WORKBOOK IN DIFFERENT SHEETS - USING THE EXPORT BUTTON ON THE BOTTOM RIGHT HAND CORNER --
-- EXPORT: PHASED_BUDGET --
select * from phased_budget;

-- EXPORT: OVERALL_BUDGET2 --
select * from Overall_budget2;

-- EXPORT: BAU_SKU --
-- RIGHT CLICK THE QUERY AND SELECT EXPORT TO FILE. RENAME AND ADD COLUMN HEADER AND PRESS EXPORT TO FILE --
select * from BAU_SKU;



------------------------------------------------------------------------------------------------------------------------
-- LOAD INTO BUDGET REPOSITORY

-- Get all weeks and periods between campaign dates
create or replace temp table dates as
select distinct  week_no, fin_period_no
from "EDWS_PROD"."PROD_CMT_PRESENTATION"."VW_DATE"
where calendar_date between dateadd(day,7,$print_start) and $final_red;

create or replace temp table dates_1 as
    select row_number() over(order by week_no) as week, * from  dates;

-- Check to see that th number of weeks between the campaign period and number of weeks in phased budget are the same
-- If not, either the phased weeks are incorrect or the dates inputted at the start are incorrect
set pb = (select max(week) from phased_budget);
set dts = (select max(week) from dates_1);
set quit = (select case when $pb <> $dts then 'CHECK PHASED BUDGET WEEKS OR CAMPAIGN DATES TO SEE WHY THE NUMBER OF WEEKS ARE NOT THE SAME' else null end as ERROR);
select $pb, $dts, $quit


-- If this errors the number of weeks are not matching up so check this
create or replace procedure runquit(PAR1 string)
returns string
language javascript
    strict
    as
    $$
        var stmt1 = snowflake.createStatement({sqlText:
        `select 1/0`});

        if (PAR1 == 'CHECK PHASED BUDGET WEEKS OR CAMPAIGN DATES TO SEE WHY THE NUMBER OF WEEKS ARE NOT THE SAME') {
        stmt1.execute();
        }

        return 'END_DT < MAX_DT';
    $$;

call runquit($quit);


create or replace temp table phased_budget_repo as
select 'Grocery'                                      as AREA,
       $CampaignName                                  as CAMPAIGN_CODE,
       $year                                          as YEAR,
       $fin_yr                                        as FIN_YEAR,
       b.fin_period_no                                as PERIOD,
       b.week_no                                      as WEEK_NUMBER,
       PRINTS,
       print_cost                                     as PRINT_COSTS,
       redemption_rate                                as RED_RATE,
       redemptions                                    as REDS,
       red_cost                                       as REDEMPTION_COST,
       red_cost_points                                as RED_COST_POINTS,
       red_cost_pounds                                as RED_COST_POUNDS,
       red_cost_with_valassis - red_cost              as VALASSIS_COST,
       target_sales_uplift                            as SALES_UPLIFT,
       SALES_UPLIFT / redemption_cost                 as TRAD_SCR,
       TRADING_PROFIT,
       TRADING_ROI,
       SALES_UPLIFT / (redemption_cost + print_costs) as JS_SCR,
       JS_PROFIT,
       JS_ROI,
       null                                           as HALO_UPLIFT,
       null                                           as TRADING_PROFIT_EXC_HALO,
       null                                           as TRADING_ROI_EXC_HALO,
       null                                           as JS_PROFIT_EXC_HALO,
       null                                           as JS_ROI_EXC_HALO,
       $customers_exl_control                         as DISTINCT_TARGET_CUSTOMERS,
       null                                           as PARTICIPATION_RATE,
       RED_COST_POINTS / 0.00425                      as POINTS_REDEEMED,
       $CampaignType                                  as CAMP_TYPE,
       $print_start                                   as PRINT_START,
       $print_stop                                    as PRINT_STOP,
       $final_red                                     as FINAL_REDEMPTION

from phased_budget a
         left join dates_1 b on a.week = b.week;



select * from  phased_budget_repo;

insert into PRODUCT_REPORTING.PRODUCT_BUDGET_REPOSITORY
select * from phased_budget_repo;


-- select distinct CAMPAIGN_CODE from PRODUCT_REPORTING.PRODUCT_BUDGET_REPOSITORY order by CAMPAIGN_CODE asc; CAMPAIGN_CODE = 'BC09CT';

