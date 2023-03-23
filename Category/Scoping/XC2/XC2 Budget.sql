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



set CampaignName    = 'X02232';         -- As per User Variable in Unica --
set CampaignType    = 'CT';
set username        = 'SH';
set control_grp     = 0.05 ;            -- percentage of control group, normally 0.08 or 0.1 --
set print_dist      = 0.60;             -- estimated print distribution --

set tab             = concat('CUSTOMER_ANALYTICS.PRODUCT_REPORTING.SH_X02232_FINAL_SEL');
select $tab;
-- ESTIMATED REDEMPTION RATE AND SCR FOR ANY CELL THAT DOESNT SIT WITHIN THE STANDARD CATEGORY AREA MANAGER --
set other_RR    = 0.05;
set other_SCR   = 2.5;

-- VARIABLES FOR BUDGET REPOSITORIES
set print_start = '2023-04-05';
set print_stop  = '2023-04-25';
set final_red   = '2023-05-16';

set year = (select year(to_date($print_start)));
set fin_yr = (select case when $print_start < '2020-03-08' then '1920'
                          when $print_start < '2021-03-07' then '2021'
                          when $print_start >= '2021-03-07' then '2122'
	      		  when $print_start >= '2022-03-06' then '2223'
	      		  when $print_start >= '2023-03-05' then '2324'
                     else '9999' end as Fin_Year);

-- if table does not have money off offers then run the below code
-- comment out any offers that are not needed
-- create or replace temp table sorted_pre as
-- select *
--        , null as bPounds1, null as bPounds2
--        , null as bPounds3, null as bPounds4
--        , null as bPounds5, null as bPounds6
--        , null as bPounds7, null as bPounds8
-- --       , null as bPounds9, null as bPounds10
-- from identifier($tab);

create or replace temp table sorted_pre as
select *
from identifier($tab);

select * from identifier($tab) limit 5;

-- IMPORT THE SELECTION FILE YOU WANT FROM CROSSCAT LIBRARY (FOR MIGRATION PURPOSES AM ONLY IMPORTING 500,000)
-- ZEROIFNULL IS APPLIED AS WHEN TRANSPOSING IF CELL IS NULL THEN ROW IS NOT ADDED TO TRANSPOSED DATA
-- RENAMING THE POINTS COLUMN AS THEY CAN'T BE SELECTED IN PIVOT FUNCTION LATER ON
create or replace temp table sorted as
select SR_ID, EC_ID, FULL_NECTAR_CARD_NUM, HH_ID
     , zeroifnull(SKU1) as SKU1, zeroifnull(SKU2) as SKU2
     , zeroifnull(SKU3) as SKU3, zeroifnull(SKU4) as SKU4
     , zeroifnull(SKU5) as SKU5, zeroifnull(SKU6) as SKU6
     , zeroifnull(SKU7) as SKU7, zeroifnull(SKU8) as SKU8
--     , zeroifnull(SKU9) as SKU9, zeroifnull(SKU10) as SKU10
     , zeroifnull(a.bPoints1) as BP1, zeroifnull(a.bPoints2) as BP2
     , zeroifnull(a.bPoints3) as BP3, zeroifnull(a.bPoints4) as BP4
     , zeroifnull(a.bPoints5) as BP5, zeroifnull(a.bPoints6) as BP6
     , zeroifnull(a.bPoints7) as BP7, zeroifnull(a.bPoints8) as BP8
--     , zeroifnull(a.bPoints9) as BP9, zeroifnull(a.bPoints10) as BP10
     , zeroifnull(substr(a.bPounds1,2,length(a.bpounds1))) as BPo1, zeroifnull(substr(a.bPounds2,2,length(a.bPounds2))) as BPo2
     , zeroifnull(substr(a.bPounds3,2,length(a.bPounds3))) as BPo3, zeroifnull(substr(a.bPounds4,2,length(a.bPounds4))) as BPo4
     , zeroifnull(substr(a.bPounds5,2,length(a.bPounds5))) as BPo5, zeroifnull(substr(a.bPounds6,2,length(a.bPounds6))) as BPo6
     , zeroifnull(substr(a.bPounds7,2,length(a.bPounds7))) as BPo7, zeroifnull(substr(a.bPounds8,2,length(a.bPounds8))) as BPo8
--     , zeroifnull(substr(a.bPounds9,2,length(a.bPounds9))) as BPo9, zeroifnull(substr(a.bPounds10,2,length(a.bPounds10))) as BPo10
     , BARCODE1, BARCODE2, BARCODE3, BARCODE4, BARCODE5, BARCODE6, BARCODE7, BARCODE8
--     , BARCODE9, BARCODE10
     , OCID1, OCID2, OCID3, OCID4, OCID5, OCID6 , OCID7, OCID8
--     , OCID9, OCID10
     , a.Scottish_Cust as Scottish_Cust
--     , LTLT_HIGH, VICS
from sorted_pre a
order by sr_id;

-- TRANSPOSES THE POINTS VALUE FOR THE SKU IN THE SELECTION FILE --
-- CODE ADDS A COUNT TO EACH CUSTOMER FOR THEIR SELECTED SKUS, SO THAT WE CAN USE THIS AND SR_ID AS AN IDENTIFIER LATER ON --
create or replace temp table trans_points as
    select sr_id, points,
           case when counts = 'BP1' then 1
               when counts = 'BP2' then 2
               when counts = 'BP3' then 3
               when counts = 'BP4' then 4
               when counts = 'BP5' then 5
               when counts = 'BP6' then 6
               when counts = 'BP7' then 7
               when counts = 'BP8' then 8
--                when counts = 'BP9' then 9
--                when counts = 'BP10' then 10
               end as count

    from sorted
    unpivot(points for counts in (BP1, BP2, BP3, BP4, BP5, BP6, BP7, BP8/*, BP9, BP10*/))
    order by sr_id, count;

-- TRANSPOSES THE SKUS IN THE SELECTION FILE AND KEEPS NECESSARY FIELDS --
-- CODE ADDS A COUNT TO EACH CUSTOMER FOR THEIR SELECTED SKUS, SO THAT WE CAN USE THIS AND SR_ID AS AN IDENTIFIER LATER ON --
create or replace temp table trans_sku as
    select sr_id, skus,
           case when counts = 'SKU1' then 1
               when counts = 'SKU2' then 2
               when counts = 'SKU3' then 3
               when counts = 'SKU4' then 4
               when counts = 'SKU5' then 5
               when counts = 'SKU6' then 6
               when counts = 'SKU7' then 7
               when counts = 'SKU8' then 8
--                when counts = 'SKU9' then 9
--                when counts = 'SKU10' then 10
               end as count

    from sorted
    unpivot(skus for counts in (sku1, sku2, sku3, sku4, sku5, sku6, sku7, sku8/*, sku9, sku10*/))
    order by sr_id, count;

create or replace temp table trans_pounds as
    select sr_id, pounds,
           case when counts = 'BPO1' then 1
               when counts = 'BPO2' then 2
               when counts = 'BPO3' then 3
               when counts = 'BPO4' then 4
               when counts = 'BPO5' then 5
               when counts = 'BPO6' then 6
               when counts = 'BPO7' then 7
               when counts = 'BPO8' then 8
--                when counts = 'BPO9' then 9
--                when counts = 'BPO10' then 10
               end as count

    from sorted
    unpivot(pounds for counts in (BPo1, BPo2, BPo3, BPo4, BPo5, BPo6, BPo7, BPo8/*, BPo9, BPo10*/))
    order by sr_id, count;


select count(distinct skus) from trans_sku;

-- MERGES THE POINTS AND SKU DATASETS ON SR_ID AND COUNT SO THE POINTS AND SKU MATCH --
create or replace temp table merged as
         select s.SR_ID, s.count, s.skus, p.points, tp.pounds, case when tp.pounds>0 then 'POUNDS' else 'POINTS' end as offer_type
         from trans_sku s
         inner join trans_points p on s.sr_id=p.sr_id and s.count=p.count
         inner join trans_pounds tp on p.sr_id=tp.sr_id and  p.count = tp.count;

select offer_type, count(*) from merged group by 1

-- ADDS ON MANAGER_RESP TO OUR TABLE --
create or replace temp table addmanager as
select    SR_ID
		, COUNT as SKU_RANK
		, SKUs as SKU
		, b.manager_resp
		, points as points
        , pounds as pounds
        , offer_type

		from merged as a
		left join "EDWS_PROD"."PROD_CMT_PRESENTATION"."VW_SKU" as b
		on a.skus=b.sku
		where end_date is null
		and skus >0;

-- ADDS ON THE AVERAGE SELLING PRICE AND THE MARGIN FOR THE SKU'S (NOTE: DOES NOT ADD FOR COUNTER ITEMS) --
create or replace temp table addaspmargin as
select    SR_ID
		, SKU_RANK
		, a.SKU
		, manager_resp
		, points
        , pounds
        , offer_type
		, avg_selling_price_26weeks as asp
		, (margin_26weeks-11.02) as margin

		from addmanager as a
		left join "EDWS_PROD"."PROD_CMT_PRESENTATION"."VW_SKU_SUMMARY" as b
		on a.sku=b.sku;

create or replace temp table addcountermanager as
         select sr_id, sku_rank, sku, points, pounds, offer_type,
         case
             when SKU <= 15 then 0.33                                           -- Override Margins for Counter skus to 0.33        -- FRESH FOOD COUNTERS
             when SKU >=235 and sku<= 249 then 0.33								-- Override Margins for Counter skus to 0.33        -- FRESH FOOD COUNTERS

             when SKU >=19 and sku<= 25 then 0.32								-- Override Margins for HOUSEHOLD skus to 0.33      -- HOUSEHOLD COUNTERS
             when SKU >=66 and sku<= 67 then 0.32								-- Override Margins for HOUSEHOLD skus to 0.33      -- HOUSEHOLD COUNTERS

             when SKU >=55 and sku<= 65 then 0.33								-- Override Margins for PRODUCE skus to 0.33        -- PRODUCE
             when SKU >=71 and sku<= 78 then 0.33								-- Override Margins for PRODUCE skus to 0.33        -- PRODUCE
             when SKU >=80 and sku<= 82 then 0.33								-- Override Margins for PRODUCE skus to 0.33        -- PRODUCE

             when SKU >=260 and sku<= 337 then 0.327							-- Override Margins for CAP skus to 0.33            -- CANNED AND PACKAGED

             when sku >=205 and sku <=216 then 0.23             		        -- based on the previous 2 MFP solus campaign       -- MFP

             when SKU in (222,223) then 0.2              		    			-- based on the criteria of margin at least 20%     -- BWS

             when SKU >=338 and sku<= 340 then 0.33								-- Override Margins for FROZEN skus to 0.33         -- FROZEN FOOD

             when SKU >=26 and sku<= 28 then 0.33								-- Override Margins for  skus to 0.33               -- HEALTH AND BEAUTY
             when SKU >=29 and sku<= 33 then 0.33								-- Override Margins for  skus to 0.33               -- HEALTH AND BEAUTY

             when SKU >=34 and sku<= 38 then 0.33               				-- Override Margins for TTD ready meals skus to 0.33 -- MEAL SOLUTIONS
             when SKU >=89 and sku<= 91 then 0.33								-- Override Margins for Plant Pioneer skus to 0.33   -- MEAL SOLUTIONS

             when sku > 340 then margin end as margin,


         case
             when SKU <= 15 then                'CM Fresh Food Counters'        -- Override Manager Resp to Fresh Food Counters
             when SKU >=235 and sku<= 249 then  'CM Fresh Food Counters'		-- Override Manager Resp to Fresh Food Counters
             when SKU >=19 and sku<= 25 then    'CM Household Counters'			-- Override Manager Resp to Household Counters
             when SKU >=66 and sku<= 67 then    'CM Household Counters'			-- Override Manager Resp to Household Counters
             when SKU >=55 and sku<= 65 then    'CM Produce Threshold'			-- Override Manager Resp to Produce
             when SKU >=71 and sku<= 78 then    'CM Produce Threshold'			-- Override Manager Resp to Produce
             when SKU >=80 and sku<= 82 then    'CM Produce Threshold'			-- Override Manager Resp to Produce
             when SKU >=260 and sku<= 337 then  'CM Packaged & Speciality'		-- Override Manager Resp to CAP
             when sku >=205 and sku <=216 then  'CM Meat  Fish & Poultry'       -- Override Manager Resp to MFP
             when SKU in (222,223) then         'CM Beers  Wines & Spirits'     -- Override Manager Resp to BWS
             when SKU >=338 and sku<= 340 then  'CM Frozen Food'				-- Override Manager Resp to Frozen Food
             when SKU >=26 and sku<= 28 then    'CM Health & Beauty' 			-- Override Manager Resp to H&B
             when SKU >=29 and sku<= 33 then    'CM Health & Beauty' 			-- Override Manager Resp to H&B
             when SKU >=34 and sku<= 38 then    'CM Meal Solutions'             -- Override Manager Resp to Meal Solutions
             when SKU >=89 and sku<= 91 then    'CM Meal Solutions'				-- Override Manager Resp to Meal Solutions
             when sku > 340 then manager_resp end as manager_resp,

        case when SKU = 1 then 2										        -- Override ASP to spend threshold --
	         when SKU = 2 then 4
             when SKU = 3 then 6
             when SKU = 4 then 1.5
             when SKU = 5 then 2.5
             when SKU = 6 then 3.5
             when SKU = 7 then 3
             when SKU = 8 then 5
             when SKU = 9 then 7
             when SKU = 10 then 3
             when SKU = 11 then 5
             when SKU = 12 then 7
             when SKU = 13 then 1
             when SKU = 14 then 1.5
             when SKU = 15 then 2

--              when SKU = 236 then 4
--              when SKU = 237 then 6
--              when SKU = 238 then 1.5
--              when SKU = 240 then 3.5
--              when SKU = 243 then 7
--              when SKU = 244 then 3
--              when SKU = 245 then 5
             when SKU = 247 then 1
             when SKU = 248 then 1.5
             when SKU = 249 then 2

             when SKU = 16 then 4
             when SKU = 17 then 6
             when SKU = 18 then 8
             when SKU = 19 then 10
             when SKU = 20 then 12.5
             when SKU = 21 then 15
             when SKU = 22 then 20
             when SKU = 23 then 25
             when SKU = 24 then 30
             when SKU = 25 then 35
             when SKU = 66 then 5
             when SKU = 67 then 7.5

             when SKU = 55 then 2
             when SKU = 56 then 2
             when SKU = 57 then 2
             when SKU = 58 then 3
             when SKU = 59 then 2
             when SKU = 60 then 3
             when SKU = 61 then 2
             when SKU = 62 then 2
             when SKU = 63 then 2
             when SKU = 64 then 2
             when SKU = 65 then 3
             when SKU >= 71 and SKU <= 73 then 4
             when SKU = 74 then 5
             when SKU >= 75 and SKU <= 78 then 4
             when SKU >= 80 and SKU <= 81 then 4
             when SKU = 82 then 5

             when sku >= 205 and sku <= 216 then 1.174946372000

             when SKU=260 then 4
             when SKU=261 then 3
             when SKU=262 then 2
             when SKU=263 then 4.25
             when SKU=264 then 3.25
             when SKU=265 then 2.5
             when SKU=266 then 5
             when SKU=267 then 4
             when SKU=268 then 2.75
             when SKU=269 then 3
             when SKU=270 then 2.5
             when SKU=271 then 1.75
             when SKU=272 then 3
             when SKU=273 then 2.5
             when SKU=274 then 2
             when SKU=275 then 3.5
             when SKU=276 then 2.5
             when SKU=277 then 1.75
             when SKU=278 then 3
             when SKU=279 then 2.5
             when SKU=280 then 2
             when SKU=281 then 5.25
             when SKU=282 then 4
             when SKU=283 then 3.25
             when SKU=284 then 2.75
             when SKU=285 then 2.25
             when SKU=286 then 1.7
             when SKU=287 then 3.5
             when SKU=288 then 2.5
             when SKU=289 then 4.25
             when SKU=290 then 4
             when SKU=291 then 3.25
             when SKU=292 then 3.25
             when SKU=293 then 2.75
             when SKU=294 then 2.25
             when SKU=295 then 3.25
             when SKU=296 then 2.6
             when SKU=297 then 2
             when SKU=298 then 2.5
             when SKU=299 then 1.75
             when SKU=300 then 1.5
             when SKU=301 then 2.5
             when SKU=302 then 2
             when SKU=303 then 4.5
             when SKU=304 then 5.5
             when SKU=305 then 3.75
             when SKU=306 then 2.75
             when SKU=307 then 5
             when SKU=308 then 4
             when SKU=309 then 3
             when SKU=310 then 2.75
             when SKU=311 then 1.75
             when SKU=312 then 6.5
             when SKU=313 then 5
             when SKU=314 then 4.5
             when SKU=315 then 4
             when SKU=316 then 3
             when SKU=317 then 3.5
             when SKU=318 then 2.75
             when SKU=319 then 1.75
             when SKU=320 then 3.75
             when SKU=321 then 3
             when SKU=322 then 2.25
             when SKU=323 then 4
             when SKU=324 then 1.5
             when SKU=325 then 5
             when SKU=326 then 2.75
             when SKU=327 then 7.25
             when SKU=328 then 5.25
             when SKU=329 then 4.25
             when SKU=330 then 5
             when SKU=331 then 4
             when SKU=332 then 4.75
             when SKU=333 then 3.75
             when SKU=334 then 3
             when SKU=335 then 3
             when SKU=336 then 3
             when SKU=337 then 2

             when SKU = 205 then 4.5
             when SKU = 206 then 3
             when SKU = 207 then 2.5
             when SKU = 208 then 7
             when SKU = 209 then 5
             when SKU = 210 then 4
             when SKU = 211 then 6
             when SKU = 212 then 4.5
             when SKU = 213 then 3.5
             when SKU = 214 then 12
             when SKU = 215 then 9
             when SKU = 216 then 5

             when SKU = 222 then 60
             when SKU = 223 then 20

             when SKU = 338 then 7.5
             when SKU = 339 then 5
             when SKU = 340 then 3.5

             when SKU = 26 then 7.5
             when SKU = 27 then 12
             when SKU = 28 then 17.5
             when SKU = 29 then 5
             when SKU = 30 then 7.5
             when SKU = 31 then 10
             when SKU = 32 then 12.5
             when SKU = 33 then 15

             when SKU = 34 then 5
             when SKU = 35 then 7.5
             when SKU = 36 then 10
             when SKU = 37 then 12.5
             when SKU = 38 then 15

             when SKU = 89 then 2.5
             when SKU = 90 then 3
             when SKU = 91 then 4

             when sku > 340 then asp end as asp
    from addaspmargin;

-- check whether any margins need to be added manually --
select distinct sku from addcountermanager
where margin is null
or manager_resp is null
or asp is null
;

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
			(case when a.sku<1000 then 1 else 0 end) as Threshold
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
	   when print_start >= '2023-03-05' then '2324'
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

select * from PRODUCT_REPORTING.PRODUCT_BUDGET_REPOSITORY where CAMPAIGN_CODE = $CampaignName
