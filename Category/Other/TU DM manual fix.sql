
use role RL_PROD_MARKETING_ANALYTICS;
use warehouse WHS_PROD_MARKETING_ANALYTICS_X2LARGE;
use database CUSTOMER_ANALYTICS;
use schema PRODUCT_REPORTING;

---------------------------------------
-- STEP 1: SET VARIABLES
---------------------------------------
-- CREATE DATE VARIABLES TO PULL ONE WEEK WORTH OF DATA

-- to_date('2022-01-10')
SET TODAYSDATE  = to_date('2022-12-19');     -- If this is not being run on a monday then change the date to monday
SET START_T     = $TODAYSDATE - 8;  -- Getting previous financial weeks first date
SET END_T       = $START_T + 6;     -- Getting previous financial weeks end date

select $TODAYSDATE;

-- GET THE FINANCIAL PERIOD AND WEEK INFORMATION AND CREATE VARIABLES --
set week_no = (select week_no from  "EDWS_PROD"."PROD_EDW_SAS_ADHOC_VIEWS"."VW_DATE_MAP" where calendar_date= $START_t);
set fin = (select fin_period_no from  "EDWS_PROD"."PROD_EDW_SAS_ADHOC_VIEWS"."VW_DATE_MAP" where calendar_date= $START_t);

show variables;




-----------------------------------------------------------------------------------------------------------------------------------
-- STEP 2: CHECK DATE - THIS IS TO SEE IF THE PRINT FILE HAS BEEN UPDATED AS WE NEED TO LATEST PRINTS FOR THIS REPORT
-----------------------------------------------------------------------------------------------------------------------------------
-- CHECK THE LATEST DATE OF THE VW_CATALINA_PRINT
set MAX_DATE = (SELECT max(Transaction_Date)
	            FROM "EDWS_PROD"."PROD_EDW_SAS_ADHOC_VIEWS"."VW_CATALINA_PRINT_FILE"
	            WHERE Transaction_Date between $START_T and $END_T);
select $MAX_DATE;

set quit = (select case when ($MAX_DATE < $END_T or $MAX_DATE is null)
            then 'ERROR: TABLE VW_CATALINA_PRINT_FILE IS NOT UP TO DATE' else 'NO ERROR' end as ERROR);
select $quit;

-- BELOW CODE WILL ERROR IF VW_CATALINA_PRINT_FILE IS NOT UP TO DATE
select case when ($MAX_DATE < $END_T or $MAX_DATE is null)
            then 1/0 else NULL end as ERROR;

-- IF QUIT = ERROR: TABLE VW_CATALINA_PRINT_FILE IS NOT UP TO DATE
select $quit;




---------------------------------------
-- STEP 3: PULL ACTIVE CAMPAIGNS
---------------------------------------



-- CREATE BASE TABLE TO PULL ALL THE RELEVANT BARCODES BASED ON DATES
CREATE OR REPLACE TEMP TABLE Base_lookup AS
SELECT *
FROM "CUSTOMER_ANALYTICS"."PRODUCT_REPORTING"."CATALINA_COUPON_LOOKUP_RAW"
WHERE print_start <= $END_T
  AND final_redemption > $START_T;

-- select * from "CUSTOMER_ANALYTICS"."PRODUCT_REPORTING"."CATALINA_COUPON_LOOKUP_RAW";

--If this errors do this to reformat the dates--
CREATE OR REPLACE TEMP TABLE Base_lookup AS
SELECT *,
       nvl(TRY_TO_DATE(print_start, 'yyyy-mm-dd'),TRY_TO_DATE(print_start,'dd/mm/yyyy')) as print_start_new,
       nvl(TRY_TO_DATE(print_stop, 'yyyy-mm-dd'),TRY_TO_DATE(print_stop,'dd/mm/yyyy')) as print_stop_new,
       nvl(TRY_TO_DATE(final_redemption, 'yyyy-mm-dd'),TRY_TO_DATE(final_redemption,'dd/mm/yyyy')) as final_redemption_new FROM "CUSTOMER_ANALYTICS"."PRODUCT_REPORTING"."CATALINA_COUPON_LOOKUP_RAW"
WHERE print_start_new <= $END_T
  AND final_redemption_new > $START_T;

select distinct campaign from base_lookup;


-- FILTER ONLY CROSS CAT AND CAT SOLUS CAMPAIGN (INCL. MFP, TU, GM AND FUEL)
CREATE OR REPLACE TEMP TABLE Cat_lookup AS
    SELECT chain_flag,
           a."order",
           program,
           award,
           audience,
           sales_rep,
           mclu,
           a.Mstrclu_Description,
           type,
           base_fee,
           trailer_fee,
           segment,
           print_start as print_start,
           print_stop as print_stop,
           final_redemption as final_redemption,
           rolling_weeks,
           face_value,
           category,
           threshold,
           a.Business_Unit,
           a.Trading_Area,
           coupon_type,
           print_estimate,
           product,
           cycle,
           trigg,
           print_to_date,
           redemptions_to_date,
           a."BARCODE" AS print_id,

           CASE
               WHEN ((CHARINDEX('GM', UPPER(a.Campaign), 1) > 0 OR CHARINDEX('HOME', UPPER(a.Campaign), 1) > 0) AND CHARINDEX('NON', UPPER(a.category), 1) > 0) THEN 'NON FOOD'
               WHEN CHARINDEX('TU', UPPER(a.Campaign), 1) > 0 AND CHARINDEX('NON', UPPER(a.category), 1) > 0 THEN 'NON FOOD'
               ELSE 'GROCERY' END AS split,

           CASE
               WHEN CHARINDEX('CROSS', UPPER(a.Campaign), 1) > 0 THEN 'CROSS CATEGORY'
               WHEN CHARINDEX('OFFER', UPPER(a.Campaign), 1) > 0 THEN 'CROSS CATEGORY'
               WHEN CHARINDEX('X-CAT', UPPER(a.Campaign), 1) > 0 THEN 'CROSS CATEGORY'
               WHEN CHARINDEX('XCAT', UPPER(a.Campaign), 1) > 0 THEN 'CROSS CATEGORY'
               WHEN CHARINDEX('X_CAT', UPPER(a.CAMPAIGN), 1) > 0 THEN 'CROSS CATEGORY'
               ELSE a.Campaign END AS campaign

        FROM Base_lookup AS a

        WHERE (UPPER(a.Campaign) LIKE '%FUEL%' AND UPPER(a.category) LIKE '%FUEL%' AND CHARINDEX('STUNT', (UPPER(a.Campaign)), 1) = 0)
           --  or  (upper(a."Campaign") like '%FUEL%' and upper(a.category) LIKE '%PETROL%' and charindex('STUNT',(upper(a."Campaign")),1)=0)
           OR ((UPPER(a.Campaign) LIKE '%GM%' OR UPPER(a.Campaign) LIKE '%HOME%') AND UPPER(a.category) LIKE '%NON%')
           OR (UPPER(a.Campaign) LIKE '%TU%' AND UPPER(a.category) LIKE '%NON%')
           OR (UPPER(a.Campaign) LIKE '%MFP%')
           OR (UPPER(a.Campaign) LIKE '%CATEGORY%')
           OR (UPPER(a.category) LIKE '%CATEGORY%')
           OR (UPPER(a.Campaign) LIKE '%2021_PP%')
           OR (UPPER(a.Campaign) LIKE '%1920_BWS%');

select distinct campaign from cat_lookup;

-- if errors then add change the names in the cat_lookup:
-- print_start_new as print_start,
-- print_stop_new as print_stop,
-- final_redemption_new as final_redemption,

CREATE OR REPLACE TEMP TABLE Cat_lookup AS
    SELECT *
        FROM Cat_lookup
        WHERE campaign <> '190514_NonFood_Tu_C4 (4605) Du' -- Removing duplicates
           OR mclu NOT IN (504878);
-- Removing these MCLUs

-- CHECK THAT ALL THE CAMPAIGNS BEING PULLED ARE CORRECTLY
SELECT campaign,
       COUNT(*) AS Freq,
       COUNT(*) / (SELECT COUNT(*) FROM Cat_lookup) AS Prop
    FROM Cat_lookup
    GROUP BY 1;


-----------------------------------------------------------
-- STEP 4: ADD IN DM AND OCC CAMPAIGNS
-----------------------------------------------------------
-- CHECK TO SEE CURRENT DM BARCODES ARE IN TABLE BELOW TO PICK UP ANY LIVE DM CAMPAIGNS --
select campaign, print_start, print_stop, final_redemption, count(distinct print_id)
from "CUSTOMER_ANALYTICS"."PRODUCT_REPORTING"."DM_CURRENT_BARCODES" group by 1,2,3,4 ORDER BY 2 DESC;


-- IF THE ABOVE TABLE DOES NOT INCLUDE THE CURRENT DM BARCODES ADD THEM IN

create or replace temp table dm_list_1 as
select substr(280050022069817,1,15) as print_id,
    '2022-11-25' as print_start,        -- CHANGE
    '2022-12-24' as print_stop,         -- CHANGE
    '2022-12-24' as final_redemption,   -- CHANGE
    'Tu Xmas DM' as Campaign;                -- CHANGE;


INSERT INTO "CUSTOMER_ANALYTICS"."PRODUCT_REPORTING"."DM_CURRENT_BARCODES"
SELECT * FROM dm_list_1;



CREATE OR REPLACE TEMP TABLE DM_LIST AS
    SELECT * FROM "CUSTOMER_ANALYTICS"."PRODUCT_REPORTING"."DM_CURRENT_BARCODES";



-- CHECK TO SEE CURRENT OCC BARCODES ARE IN TABLE BELOW TO PICK UP ANY LIVE DM CAMPAIGNS --
select campaign, print_start, print_stop, final_redemption, count(distinct print_id)
from "CUSTOMER_ANALYTICS"."PRODUCT_REPORTING"."OCC_CURRENT_BARCODES"
group by 1,2,3,4 ORDER BY 2 DESC;

/*
-- IF THE ABOVE TABLE DOES NOT INCLUDE THE CURRENT DM BARCODES ADD THEM IN

CREATE OR REPLACE TEMP TABLE OCC1 AS
SELECT DISTINCT SUBSTR(a.Barcode, 1, 15) AS print_id,           -- CHANGE
                '2022-02-09'             AS print_start,        -- CHANGE
                '2022-03-05'             AS print_stop,         -- CHANGE
                '2022-03-05'             AS final_redemption,   -- CHANGE
                'GOL RET WAVE II OCC'   AS Campaign            -- CHANGE
FROM product_reporting.PRODUCT_GOL_RET_WAVE_2_OCC_BARCODES AS a; -- CHANGE

INSERT INTO "CUSTOMER_ANALYTICS"."PRODUCT_REPORTING"."OCC_CURRENT_BARCODES"
SELECT * FROM OCC1;

*/

CREATE OR REPLACE TEMP TABLE OCC AS
    SELECT * FROM "CUSTOMER_ANALYTICS"."PRODUCT_REPORTING"."OCC_CURRENT_BARCODES";




/*
select distinct barcode from
(select * from EDWS_PROD.PROD_CMT_PRESENTATION.vw_catalina_print_file
where barcode in (select print_id from HSV))
*/



-----------------------------------------------------------
-- STEP 5: JOIN THE C@T, DM & OCC CAMPAIGNS
-----------------------------------------------------------
-- MAKE SURE DATATYPES IN TABLES WE WANT TO JOIN ARE THE SAME
ALTER TABLE dm_list
    ADD "order" bigint , chain_flag VARCHAR(10)
    , program BIGINT
    , award BIGINT
    , audience VARCHAR(100)
    , sales_rep VARCHAR(1000)
    , mclu BIGINT
    , Mstrclu_Description VARCHAR(10000)
    , "type" VARCHAR(10)
    , base_fee DOUBLE PRECISION
    , trailer_fee VARCHAR(10)
    , segment VARCHAR(10000)
    , rolling_weeks BIGINT
    , face_value DOUBLE PRECISION
    , category VARCHAR(1000)
    , threshold VARCHAR(1000)
    , Business_Unit VARCHAR(1000)
    , Trading_Area VARCHAR(1000)
    , coupon_type VARCHAR(1000)
    , print_estimate BIGINT
    , product VARCHAR(10000)
    , cycle VARCHAR(50)
    , trigg BIGINT
    , print_to_date BIGINT
    , redemptions_to_date BIGINT;

ALTER TABLE OCC
    ADD "order" bigint , chain_flag VARCHAR(10)
    , program BIGINT
    , award BIGINT
    , audience VARCHAR(100)
    , sales_rep VARCHAR(1000)
    , mclu BIGINT
    , Mstrclu_Description VARCHAR(10000)
    , "type" VARCHAR(10)
    , base_fee DOUBLE PRECISION
    , trailer_fee VARCHAR(10)
    , segment VARCHAR(10000)
    , rolling_weeks BIGINT
    , face_value DOUBLE PRECISION
    , category VARCHAR(1000)
    , threshold VARCHAR(1000)
    , Business_Unit VARCHAR(1000)
    , Trading_Area VARCHAR(1000)
    , coupon_type VARCHAR(1000)
    , print_estimate BIGINT
    , product VARCHAR(10000)
    , cycle VARCHAR(50)
    , trigg BIGINT
    , print_to_date BIGINT
    , redemptions_to_date BIGINT;


-- COMBINING CAT, DM & OCC CAMPAIGNS - TAKING ONLY THOSE WHERE REDEMPTIONS ARE STILL VALID
CREATE OR REPLACE TEMP TABLE final_lookup AS
-- C@T CAMPAIGNS
    SELECT "order",
           chain_flag,
           program,
           award,
           audience,
           sales_rep,
           mclu,
           Mstrclu_Description,
           type,
           base_fee,
           trailer_fee,
           segment,
           print_start,
           print_stop,
           final_redemption,
           rolling_weeks,
           face_value,
           category,
           threshold,
           Business_Unit,
           Trading_Area,
           coupon_type,
           print_estimate,
           product,
           cycle,
           trigg,
           print_to_date,
           redemptions_to_date,
           split,
           campaign,
           print_id
        FROM Cat_lookup
        WHERE TO_DATE(final_redemption) >= $END_T
           OR TO_DATE(final_redemption) BETWEEN $START_T AND $END_T
    UNION ALL
-- DM CAMPAIGNS
    SELECT "order",
           chain_flag,
           program,
           award,
           audience,
           sales_rep,
           mclu,
           Mstrclu_Description,
           "type",
           base_fee,
           trailer_fee,
           segment,
           TO_DATE(print_start) AS print_start,
           TO_DATE(print_stop) AS print_stop,
           TO_DATE(final_redemption) AS final_redemption,
           rolling_weeks,
           face_value,
           category,
           threshold,
           Business_Unit,
           Trading_Area,
           coupon_type,
           print_estimate,
           product,
           cycle,
           trigg,
           print_to_date,
           redemptions_to_date,
           'GROCERY' AS split,
           campaign,
           print_id
        FROM dm_list
        WHERE TO_DATE(final_redemption) >= $END_T
           OR TO_DATE(final_redemption) BETWEEN $START_T AND $END_T
    UNION ALL
-- OCC CAMPAIGNS
    SELECT "order",
           chain_flag,
           program,
           award,
           audience,
           sales_rep,
           mclu,
           Mstrclu_Description,
           "type",
           base_fee,
           trailer_fee,
           segment,
           TO_DATE(print_start) AS print_start,
           TO_DATE(print_stop) AS print_stop,
           TO_DATE(final_redemption) AS final_redemption,
           rolling_weeks,
           face_value,
           category,
           threshold,
           Business_Unit,
           Trading_Area,
           coupon_type,
           print_estimate,
           product,
           cycle,
           trigg,
           print_to_date,
           redemptions_to_date,
           'GROCERY' AS split,
           campaign,
           print_id
        FROM OCC
        WHERE TO_DATE(final_redemption) >= $END_T
           OR TO_DATE(final_redemption) BETWEEN $START_T AND $END_T;


SELECT Campaign,
       COUNT(*) AS Freq,
       COUNT(*) / (SELECT COUNT(*) FROM final_lookup) AS Prop
    FROM final_lookup AS a
    GROUP BY 1
    ORDER BY 1;

SELECT *
    FROM final_lookup LIMIT 100;
-----------------------------------------------------------
-- STEP 6: DEDUPING ON BARCODE LEVEL
-----------------------------------------------------------
CREATE OR REPLACE TEMP TABLE final_lookup AS
    SELECT a."order",
           CHAIN_FLAG,
           PROGRAM,
           AWARD,
           AUDIENCE,
           SALES_REP,
           MCLU,
           a.Mstrclu_Description,
           TYPE,
           BASE_FEE,
           TRAILER_FEE,
           SEGMENT,
           PRINT_START,
           PRINT_STOP,
           FINAL_REDEMPTION,
           ROLLING_WEEKS,
           FACE_VALUE,
           CATEGORY,
           THRESHOLD,
           a.Business_Unit,
           a.Trading_Area,
           COUPON_TYPE,
           a.PRINT_ESTIMATE,
           PRODUCT,
           CYCLE,
           TRIGG,
           PRINT_TO_DATE,
           REDEMPTIONS_TO_DATE,
           SPLIT,
           CAMPAIGN,
           PRINT_ID
        FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY PRINT_ID ORDER BY PRINT_STOP DESC) AS rn FROM final_lookup) a
        WHERE rn = 1;

-- CHECK IF THERE ARE STILL DUPLICATES --
SELECT print_id,
       COUNT(*)
    FROM final_lookup
    GROUP BY 1
    HAVING COUNT(*) > 1;

select distinct campaign from final_lookup;

-------------------------------------------------------------------------------------------
-- STEP 7: SETUP THE LIST OF COUPONS AND CREATE A PARAMETER HOLDING ALL THE COUPON IDS
-------------------------------------------------------------------------------------------
CREATE OR REPLACE TEMP TABLE F01_Coupons AS
    SELECT campaign,
           CASE
               WHEN SUBSTRING(Print_id, 1, 2) = '28' THEN 'Fixed Points'
               WHEN SUBSTRING(Print_id, 1, 2) = '43' THEN 'Petrol'
               WHEN SUBSTRING(Print_id, 1, 2) = '99' THEN 'Money-off'
               WHEN SUBSTRING(Print_id, 1, 2) = '29' THEN 'Points Multiplier'
               WHEN LEFT(PRINT_ID,1) = 'E' THEN 'EVOUCHER' END AS Coupon_Type,
           print_id,
           split,
           CASE
               WHEN SUBSTRING(Print_id, 1, 2) = '28' THEN SUBSTRING(Print_id, 8, 7)
               WHEN SUBSTRING(Print_id, 1, 2) = '43' THEN CONCAT('000000', SUBSTRING(Print_id, 1, 7))
               WHEN SUBSTRING(Print_id, 1, 2) = '99' THEN Print_id
               WHEN SUBSTRING(Print_id, 1, 2) = '29' THEN SUBSTRING(Print_id, 6, 7)
               WHEN LEFT(PRINT_ID,1) = 'E' THEN print_id END AS Coupon_ID,
           ROW_NUMBER() OVER (ORDER BY print_id ASC ) AS cts
        FROM final_lookup
        WHERE print_id <> '0';


delete from F01_Coupons where Coupon_ID = '';

SELECT * FROM F01_Coupons limit 100;

-- CREATE MACRO TO CHECK THE TOTAL NUMBER OF COUPONS --
SET max_cts = (SELECT MAX(cts)
                   FROM F01_Coupons);
SELECT $max_cts;

CREATE OR REPLACE TEMP TABLE F01_Coupons AS
    SELECT CAMPAIGN,
           COUPON_TYPE,
           PRINT_ID,
           SPLIT,
           CASE WHEN COUPON_TYPE ='EVOUCHER' THEN NULL ELSE cast(a."COUPON_ID" as number(15,0)) END as coupon_id,
           a."CTS"
        FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY COUPON_ID ORDER BY COUPON_ID DESC) AS rn FROM F01_Coupons) a
        WHERE rn = 1;

select * from PRODUCT_REPORTING.F01_Coupons;

--------------------------------------------------------------------------------------------------------------------------
-- STEP 8: CREATE LOOKUP TABLES FOR THE BARCODES BASED ON COUPON TYPE - WILL BE USED FOR THE REDEMPTIONS AND PRINTS
--------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TEMP TABLE Coupon_List_MOF AS
    SELECT TRIM(coupon_id) AS coupon_id
        FROM F01_Coupons
        WHERE Coupon_Type IN ('Money-off', 'Petrol');

CREATE OR REPLACE TEMP TABLE Coupon_List_PTS AS
    SELECT TRIM(coupon_id) AS coupon_id
        FROM F01_Coupons
        WHERE Coupon_Type IN ('Points Multiplier', 'Fixed Points');

CREATE OR REPLACE TEMP TABLE Coupon_List_OCC AS
    SELECT TRIM(coupon_id) AS coupon_id
        FROM F01_Coupons
        WHERE Coupon_Type IN ('Points Multiplier', 'Fixed Points', 'Money-off');

CREATE OR REPLACE TEMP TABLE Print_ID_1 AS
    SELECT TRIM(Print_ID) AS Print_ID
        FROM F01_Coupons;

CREATE OR REPLACE TEMP TABLE COUPON_LIST_EVOUCHER AS
    SELECT TRIM(print_id) AS PRINT_ID FROM F01_Coupons WHERE Coupon_Type = 'EVOUCHER'


SELECT * FROM F01_Coupons where campaign = 'GOLRET1';
-----------------------------------------------------------
-- STEP 9: COUPON PRINTS
-----------------------------------------------------------

CREATE OR REPLACE TEMP TABLE G11_Prints AS
    SELECT DISTINCT *
        FROM "EDWS_PROD"."PROD_EDW_SAS_ADHOC_VIEWS"."VW_CATALINA_PRINT_FILE" a
        WHERE a."BARCODE" IN (SELECT * FROM Print_ID_1)
          AND Transaction_Date BETWEEN $START_t AND $End_t;



CREATE OR REPLACE TEMP TABLE tot_prints AS
    SELECT a."BARCODE" AS Print_id,
           COUNT(*)    AS prints
    FROM G11_Prints a
    GROUP BY 1;

-----------------------------------------------------------
-- STEP 10: COUPON REDEMPTIONS
-----------------------------------------------------------
-- *********************** POINTS COUPON REDEMPTIONS *********************** --
CREATE OR REPLACE TEMP TABLE G01_Points_Redemp AS
    SELECT Party_Account_ID,
           Coupon_ID,
           Points_Earned,
           Transaction_Date
        FROM "EDWS_PROD"."PROD_EDW_SAS_ADHOC_VIEWS"."VW_LOYALTY_COUPON_REDEMPTION"
        WHERE Coupon_ID IN (SELECT coupon_id FROM Coupon_List_PTS)
          AND Points_Earned > 0
          AND Transaction_Date BETWEEN $START_t AND $End_t;


CREATE OR REPLACE TEMP TABLE tot_pts_reds AS
    SELECT Coupon_ID,
           SUM(Points_Earned) AS pts,
           COUNT(*) AS reds,
           MAX(Transaction_Date) AS x
        FROM G01_Points_Redemp
        GROUP BY 1;

select max(transaction_date) from G01_Points_Redemp;

-- *********************** PETROL AND MONEY OFF COUPON REDEMPTIONS *********************** --
CREATE OR REPLACE TEMP TABLE H01_MOP_Redemp AS
    SELECT Party_Account_ID,
           Coupon_ID,
           Payment_Value,
           Transaction_Date
        FROM "EDWS_PROD"."PROD_EDW_SAS_ADHOC_VIEWS"."VW_PAYMENT_LINE"
        WHERE (Coupon_ID IN (SELECT Coupon_ID FROM Coupon_List_MOF)
                   OR SUBSTRING(Coupon_ID, 1, 12) IN (SELECT Coupon_ID FROM Coupon_List_MOF)
                   OR SUBSTRING(COUPON_ID,7,7) IN (SELECT COUPON_ID FROM Coupon_List_MOF))
          AND Transaction_Date BETWEEN $START_t AND $End_t;

select Coupon_ID, COUNT(*) from H01_MOP_Redemp GROUP BY 1;

CREATE OR REPLACE TEMP TABLE tot_reds AS
    SELECT Coupon_ID,
           SUM(Payment_Value) AS money_off,
           COUNT(*) AS reds,
           MAX(Transaction_Date) AS x
        FROM H01_MOP_Redemp
        GROUP BY 1
        ORDER BY 1;

SELECT *
    FROM tot_reds;


-- *********************** OCC REDEMPTIONS *********************** --
CREATE OR REPLACE TEMP TABLE OCC_REDS AS
    SELECT COUPON_ID,
           O.*
        FROM "EDWS_PROD"."PROD_EDW_SAS_ADHOC_VIEWS"."VW_COUPON_DETAILS" C
        INNER JOIN "EDWS_PROD"."PROD_EDW_SAS_ADHOC_VIEWS"."VW_OC_REDEMPTION" O ON C.OC_ID = O.OC_ID
        WHERE CAST(COUPON_ID AS int) IN (SELECT CAST(COUPON_ID AS int) FROM Coupon_List_OCC)
          AND CAST(TransactionDateTime AS DATE) BETWEEN $START_t AND $End_t;

select * from "EDWS_PROD"."PROD_EDW_SAS_ADHOC_VIEWS"."VW_OC_REDEMPTION" where length(oc_id)<5 limit 100
select * from edws_prod.prod_cmt_presentation.vw_redeemed_voucher limit 100;

CREATE OR REPLACE TEMP TABLE tot_occ AS
    SELECT coupon_id,
           SUM(bonus_points_earned) AS pts,
           SUM(discount_value) AS pds,
           COUNT(*) AS reds
        FROM OCC_REDS
        GROUP BY 1;

-- *********************** EVOUCHER REDEMPTIONS *********************** --
CREATE OR REPLACE TEMP TABLE EVOUCHER_REDS AS
    SELECT SR_ID, VOUCHER_CODE AS COUPON_ID, CUSTOMER_NUMBER, ORDER_NUMBER, REDEEMED_DATE,
           VOUCHER_TYPE, DESCRIPTION, ADDITIONAL_POINTS, DISCOUNT_VALUE, SPEND_THRESHOLD,
           START_DATE, END_DATE
        FROM edws_prod.prod_cmt_presentation.vw_redeemed_voucher O
        WHERE VOUCHER_CODE IN (SELECT PRINT_ID FROM Coupon_List_EVOUCHER)
          AND CAST(REDEEMED_DATE AS DATE) BETWEEN $START_t AND $End_t;

select * from "EDWS_PROD"."PROD_EDW_SAS_ADHOC_VIEWS"."VW_OC_REDEMPTION" where length(oc_id)<5 limit 100
select * from edws_prod.prod_cmt_presentation.vw_redeemed_voucher  limit 100;

CREATE OR REPLACE TEMP TABLE tot_EVOUCHER AS
    SELECT coupon_id,
           SUM(ADDITIONAL_POINTS) AS pts,
           SUM(discount_value) AS pds,
           COUNT(*) AS reds
        FROM EVOUCHER_REDS
        GROUP BY 1;

SELECT * FROM tot_EVOUCHER LIMIT 200;

-----------------------------------------------------------
-- STEP 11: CREATE A SUMMARY TABLE
-----------------------------------------------------------
CREATE OR REPLACE TEMP TABLE TOTAL_by_barcode AS
    SELECT a.Print_id,
           a.coupon_id,
           CASE
               WHEN a.coupon_type = 'Petrol' THEN b.money_off END AS petrol_cost,
           CASE
               WHEN a.coupon_type = 'Money-off' THEN COALESCE(b.money_off, e.money_off) END AS money_off_cost,
           c.pts,
           COALESCE(f.pts, G.PTS) AS occ_pts,
           COALESCE(f.pds, G.PDS) AS occ_pds,
           d.prints,
           COALESCE(b.reds, c.reds, e.reds) AS redemption,
           COALESCE(f.reds, G.REDS) AS occ_reds,
           a.campaign,
           a.split
        FROM F01_Coupons a
        LEFT JOIN tot_reds b ON a.Coupon_ID = b.Coupon_ID
        LEFT JOIN tot_reds E ON a.Coupon_ID = SUBSTR(e.Coupon_ID, 1, 12) -- needed as the PP money off coupons are shorted in coupon lookup
        LEFT JOIN tot_pts_reds c ON a.coupon_id = c.coupon_id
        LEFT JOIN tot_prints d ON a.Print_id = d.Print_id
        LEFT JOIN tot_occ f ON a.coupon_id = f.coupon_id
        LEFT JOIN tot_EVOUCHER G ON A.print_id=G.coupon_id
        WHERE b.money_off > 0
           OR c.pts > 0
           OR d.prints > 0
           OR e.money_off > 0
           OR f.pts > 0
           OR f.pds > 0
            OR G.pts >0
            OR G.PDS >0;

SELECT * FROM TOTAL_by_barcode  WHERE Print_id ILIKE 'E%';
SELECT * FROM F01_Coupons WHERE Print_id = 'EHPYVXNHFY6N'
select * from tot_EVOUCHER where coupon_id ILIKE 'E%';
SELECT * FROM TOTAL_by_barcode where campaign='21/22_TuC13_CP (5950)';

-- REPLACE ALL THE NULLS WITH 0
CREATE OR REPLACE TEMP TABLE TOTAL_by_barcode AS
    SELECT print_id,
           coupon_id,
           ZEROIFNULL(petrol_cost) AS petrol_cost,
           ZEROIFNULL(money_off_cost) + ZEROIFNULL(occ_pds) AS money_off_cost,
           ZEROIFNULL(pts) AS pts,
           ZEROIFNULL(occ_pts) AS occ_pts,
           ZEROIFNULL(occ_pds) AS occ_pds,
           ZEROIFNULL(prints) AS prints,
           ZEROIFNULL(redemption) AS redemption,
           ZEROIFNULL(occ_reds) AS occ_reds,
           Campaign,
           split
        FROM TOTAL_by_barcode;

SELECT * FROM TOTAL_by_barcode where coupon_id = 4949363;

CREATE OR REPLACE TEMP TABLE TOTAL_by_campaign AS
    SELECT campaign,
           split,
           ZEROIFNULL(SUM(prints)) AS no_of_prints,
           ZEROIFNULL(SUM(prints) * 0.0133) AS print_cost,
           ZEROIFNULL(SUM(redemption)) AS no_of_reds,
           ZEROIFNULL(SUM(redemption) * 0.0142) AS valassis_cost,
           ZEROIFNULL(SUM(occ_reds)) AS no_of_occ_reds,
           ZEROIFNULL(SUM(pts)) AS points_issued,
           ZEROIFNULL((SUM(pts) * 0.00425)) AS points_cost,
           ZEROIFNULL(SUM(occ_pts)) AS occ_points_issued,
           ZEROIFNULL((SUM(occ_pts) * 0.00425)) AS occ_points_cost,
           ZEROIFNULL(SUM(petrol_cost)) AS petrol_cost,
           ZEROIFNULL(SUM(money_off_cost)) AS money_off_cost,
           ZEROIFNULL(COUNT(DISTINCT Print_id)) AS no_of_barcode
        FROM TOTAL_by_barcode
        GROUP BY 1, 2;



CREATE OR REPLACE TEMP TABLE TOTAL_week AS
    SELECT split,
           ZEROIFNULL(SUM(no_of_prints)) AS no_of_prints,
           ZEROIFNULL(SUM(print_cost)) AS print_cost,
           ZEROIFNULL(SUM(no_of_reds)) AS no_of_reds,
           ZEROIFNULL(SUM(valassis_cost)) AS valassis_cost,
           ZEROIFNULL(SUM(no_of_occ_reds)) AS no_of_occ_reds,
           ZEROIFNULL(SUM(points_issued)) AS points_issued,
           ZEROIFNULL(SUM(points_cost)) AS points_cost,
           ZEROIFNULL(SUM(occ_points_issued)) AS occ_points_issued,
           ZEROIFNULL(SUM(occ_points_cost)) AS occ_points_cost,
           ZEROIFNULL(SUM(petrol_cost)) AS petrol_cost,
           ZEROIFNULL(SUM(money_off_cost)) AS money_off_cost,
           ZEROIFNULL(SUM(no_of_barcode)) AS no_of_barcode
        FROM TOTAL_by_campaign
        GROUP BY 1;

SELECT * FROM TOTAL_by_campaign;
-- select * from total_week;
-----------------------------------------------------------
-- STEP 12: ADDING TO MASTER TABLE
-----------------------------------------------------------
-- WILL ERROR IF THIS WEEKS INFO IS ALREADY IN THE MASTER TABLE
-- SET weeknum = (SELECT MAX(week_no)
--                    FROM "CUSTOMER_ANALYTICS"."PRODUCT_REPORTING"."WEEKLY_PNR_2223_VAL");
SET weeknum = 202240;
SELECT $weeknum;

SELECT *
    FROM "CUSTOMER_ANALYTICS"."PRODUCT_REPORTING"."WEEKLY_PNR_2223_VAL";

SET master_flag = (SELECT CASE
                              WHEN ($week_no <= $weeknum) THEN 'ERROR: THIS WEEK HAS BEEN PUT IN THE MASTER TABLE, DOUBLE CHECK PLEASE'
                              ELSE 'NO ERROR: ADD TO MASTER TABLE' END AS message);
SELECT $master_flag;

-- Stored procedure will error if master_flag = ERROR: THIS WEEK HAS BEEN PUT IN THE MASTER TABLE, DOUBLE CHECK PLEASE --
CREATE OR REPLACE PROCEDURE master_table(PAR1 string) RETURNS string
    LANGUAGE JAVASCRIPT STRICT AS
$$
var stmt1 = snowflake.createStatement({
    sqlText:
        `select 1/0`
});
if (PAR1 == 'ERROR: THIS WEEK HAS BEEN PUT IN THE MASTER TABLE, DOUBLE CHECK PLEASE') {
    stmt1.execute();
}

return 'OK TO ADD TO MASTER TABLE';
$$;

CALL master_table($master_flag);


-- CONTINUE RUNNING IF NO ERROR ABOVE
CREATE OR REPLACE TABLE "CUSTOMER_ANALYTICS"."PRODUCT_REPORTING"."WEEKLY_PNR_2223_VAL" AS
    SELECT *
        FROM "CUSTOMER_ANALYTICS"."PRODUCT_REPORTING"."WEEKLY_PNR_2223_VAL"
    UNION ALL
    SELECT *,
           $week_no AS week_no,
           $fin AS financial_period
        FROM TOTAL_by_campaign;

select * from  "CUSTOMER_ANALYTICS"."PRODUCT_REPORTING"."WEEKLY_PNR_2223_VAL";



-----------------------------------------------------------
-- STEP 13: SUMMARISE RESULTS
-----------------------------------------------------------
-- CALCULATE THE TOTAL IN THE CURRENT PERIOD
CREATE OR REPLACE TEMP TABLE RUNNING_TOTAL_fin AS
    SELECT split,
           SUM(print_cost) AS print_cost,
           SUM(valassis_cost) AS valassis_cost,
           SUM(points_cost) AS points_cost,
           SUM(points_issued) AS points_issued,
           SUM(occ_points_issued) AS occ_points_issued,
           SUM(occ_points_cost) AS occ_points_cost,
           SUM(petrol_cost) AS petrol_cost,
           SUM(money_off_cost) AS money_off_cost
        FROM "CUSTOMER_ANALYTICS"."PRODUCT_REPORTING"."WEEKLY_PNR_2223_VAL"
        WHERE financial_period = $fin
        GROUP BY 1;


-- CALCULATE THE TOTAL IN THE CURRENT FINANCIAL YEAR
CREATE OR REPLACE TEMP TABLE RUNNING_TOTAL_YTD AS
    SELECT split,
           SUM(print_cost) AS print_cost,
           SUM(valassis_cost) AS valassis_cost,
           SUM(points_cost) AS points_cost,
           SUM(points_issued) AS points_issued,
           SUM(occ_points_issued) AS occ_points_issued,
           SUM(occ_points_cost) AS occ_points_cost,
           SUM(petrol_cost) AS petrol_cost,
           SUM(money_off_cost) AS money_off_cost
        FROM "CUSTOMER_ANALYTICS"."PRODUCT_REPORTING"."WEEKLY_PNR_2223_VAL"
        GROUP BY 1;


-- CALCULATE THE TOTAL BY CAMPAIGN IN THE CURRENT PERIOD
CREATE OR REPLACE TEMP TABLE RUNNING_TOTAL_fin_camp AS
    SELECT Campaign,
           split,
           SUM(print_cost) AS print_cost,
           SUM(valassis_cost) AS valassis_cost,
           SUM(points_cost) AS points_cost,
           SUM(points_issued) AS points_issued,
           SUM(occ_points_issued) AS occ_points_issued,
           SUM(occ_points_cost) AS occ_points_cost,
           SUM(petrol_cost) AS petrol_cost,
           SUM(money_off_cost) AS money_off_cost
        FROM "CUSTOMER_ANALYTICS"."PRODUCT_REPORTING"."WEEKLY_PNR_2223_VAL" a
        WHERE financial_period = $fin
        GROUP BY 1, 2;


-- CALCULATE THE TOTAL BY CAMPAIGN IN THE CURRENT FINANCIAL YEAR
CREATE OR REPLACE TEMP TABLE RUNNING_TOTAL_YTD_camp AS
    SELECT Campaign,
           split,
           SUM(print_cost) AS print_cost,
           SUM(valassis_cost) AS valassis_cost,
           SUM(points_cost) AS points_cost,
           SUM(points_issued) AS points_issued,
           SUM(occ_points_issued) AS occ_points_issued,
           SUM(occ_points_cost) AS occ_points_cost,
           SUM(petrol_cost) AS petrol_cost,
           SUM(money_off_cost) AS money_off_cost
        FROM "CUSTOMER_ANALYTICS"."PRODUCT_REPORTING"."WEEKLY_PNR_2223_VAL" a
        GROUP BY 1, 2;



-----------------------------------------------------------
-- STEP 14: EXPORT THE DATA
-----------------------------------------------------------
-- Export the tables below into an excel workbook following the format of previous category cost reports
-- Name the sheets the headings above the code

-- TOT_week_wkno
SELECT *
    FROM TOTAL_week;

-- TOT_camp_wkno
SELECT *
    FROM TOTAL_by_campaign;

-- TOT_barcode_wkno
SELECT *
    FROM TOTAL_by_barcode;

select count(*) from TOTAL_by_barcode;

CREATE OR REPLACE TABLE CCR_Barcodes AS
    SELECT *
        FROM TOTAL_by_barcode;

-- YTD_camp_wkno
SELECT *
    FROM RUNNING_TOTAL_YTD_camp;

-- TOT_fin_period_wkno
SELECT *
    FROM RUNNING_TOTAL_fin_camp;

-- YTD_wkno
SELECT *
    FROM RUNNING_TOTAL_YTD;

-- TOT_fin_period_wkno
SELECT *
    FROM RUNNING_TOTAL_fin;

------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------
-- STEP 15: DO NOT RUN delete from master table
-----------------------------------------------------------
-- IF YOU ACCIDENTALLY RAN THE CODE MORE THAN ONCE, THEN RUN THIS CODE BEFORE RERUNNING THE WHOLE CODE ABOVE

create or replace table "CUSTOMER_ANALYTICS"."PRODUCT_REPORTING"."WEEKLY_PNR_2223_VAL" as
select *
from "CUSTOMER_ANALYTICS"."PRODUCT_REPORTING"."WEEKLY_PNR_2223_VAL"
where week_no <> $week_no;


CREATE OR REPLACE TABLE "CUSTOMER_ANALYTICS"."PRODUCT_REPORTING"."WEEKLY_PNR_2223_VAL_BK_UP" AS
    SELECT * FROM "CUSTOMER_ANALYTICS"."PRODUCT_REPORTING"."WEEKLY_PNR_2223_VAL";


select * from "CUSTOMER_ANALYTICS"."PRODUCT_REPORTING"."WEEKLY_PNR_2223_VAL"
where week_no = $week_no;

select * from "CUSTOMER_ANALYTICS"."PRODUCT_REPORTING"."WEEKLY_PNR_2223_VAL"
order by week_no desc;
