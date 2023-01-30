/* -------------------------------------------------------------------
--------------------LOAD IN OFFERS TO OFFER BANK----------------------
----------------------------------------------------------------------*/

-- setting macros
use role RL_PROD_MARKETING_ANALYTICS;
use database CUSTOMER_ANALYTICS;
use warehouse WHS_PROD_MARKETING_ANALYTICS_X2LARGE;
use schema PRODUCT_REPORTING;

--IMPORT A CSV WITH THE FOLLOWING COLUMNS:
--SKU, PRODUCT_DESC, OFFER_CELL, POINTS, POUNDS, THRESHOLD_AMOUNT, THRESHOLD_QUANTITY, BARCODE, PRINT_START
--the remaining 4 column in the product_xc_offerbank are created using the code below
--load in barcodes raised by comms as a csv (ensure start date as date and barcode as varchar)
--check looks as expected
set new_off = '"CUSTOMER_ANALYTICS"."PRODUCT_REPORTING"."XC1_Offer_Bank6"'; -- change to correct table name

select * from identifier($new_off);

-- create or replace table product_xc_offerbank_2324
-- ("SKU" varchar(50),
-- "PRODUCT_DESC" varchar(1000),
-- "OFFER_CELL" varchar(50),
-- "POINTS" varchar(50),
-- "POUNDS" varchar(50),
-- "THRESHOLD_AMOUNT" varchar(50),
-- "THRESHOLD_QUANTITY" varchar(50),
-- "BARCODE" varchar(50),
-- "COUPON_ID" varchar(50),
-- "OC_ID" varchar(50),
-- "START_DATE" DATE,
-- "EXPIRY_DATE" DATE,
-- "DATE_ADDED" DATE
-- );

--add in to offerbank table
insert into         product_xc_offerbank_2324
                    (
                    sku, product_desc, offer_cell,
                    points, pounds, threshold_amount,
                    threshold_quantity, barcode, start_date,
                    expiry_date, date_added
                    )
select              *,
                    dateadd(month, 24, "PRINT_START") as expiry_date, -- expiry date 2 years after start date
                    current_date() as date_added
from identifier($new_off);

create or replace temp table coup_auth_and_ocid as
    select a.barcode, b.coupon_id, b.oc_id
           from product_xc_offerbank_2324 a
inner join EDWS_PROD.prod_cmt_presentation.vw_coupon_details b on a.barcode=b.barcode;



create or replace table product_xc_offerbank_2324 as
    select a.sku, a.product_desc, a.offer_cell,
                    a.points, a.pounds, a.threshold_amount,
                    a.threshold_quantity, a.barcode,
           b.coupon_id, b.oc_id, a.start_date,
                    a.expiry_date, a.date_added
from product_xc_offerbank_2324 a
inner join coup_auth_and_ocid b
    on a.barcode=b.barcode;

select * from product_xc_offerbank_2324;


-- check that they have been entered correctly
select * from product_xc_offerbank_2324 where date_added = current_date();

-- check for any duplicate rows on sku, points, pounds and barcode - remove where necessary
select              *
from                (
                    select              sku, product_desc, offer_cell, points, pounds, barcode,
                                        row_number() over(partition by sku, points, pounds, barcode ORDER BY sku) as rn
                    from                product_xc_offerbank
                    )
where               rn = 2
;


-- create a back up table, in case incorrect coupons are deleted when de-duping

create or replace table product_xc_offerbank_2324_bk_up as
    select * from product_xc_offerbank_2324;

--if the above query returns rows there is duplicates so uncomment and run this query to get only unique rows
--not sure it will work if they have a different date added though
-- create or replace table product_xc_offerbank_2324 as
-- select sku, product_desc, offer_cell,
--                     points, pounds, threshold_amount,
--                     threshold_quantity, barcode, start_date,
--                     expiry_date, date_added
-- from (select sku, product_desc, offer_cell,
--                     points, pounds, threshold_amount,
--                     threshold_quantity, barcode, start_date,
--                     expiry_date, date_added,  row_number() over(partition by sku, points, pounds, barcode ORDER BY sku) as rn
--     from PRODUCT_XC_OFFERBANK_2324 )
-- where rn=1;


--drop table that was imported to snowflake as no longer needed
drop table identifier($new_off);


/* -------------------------------------------------------------------
--------------------MATCH TO EXISTING OFFERS--------------------------
----------------------------------------------------------------------*/

-- setting macros
use role MARKETING_CUSTOMER_ANALYTICS;
use database CUSTOMER_ANALYTICS;
use warehouse ADW_2XLARGE_ADHOC_WH;
use schema PRODUCT_REPORTING;


-- comms will send a file of xc offer mechanics that need to be matched to the offerbank to see where we need to raise new barcodes
-- the table must have the fields offer_cell, sku, product_desc, threshold_amount, threshold_quantity, points, pounds (even if any of these columns are null)
-- load this table into snowflake as a csv and name appropriately

-- setting table variable
set match_off = '"CUSTOMER_ANALYTICS"."PRODUCT_REPORTING"."IR_matching_practice"'; -- change to correct table name
--added in threshold amount and some pounds/threshold offers to check
--need null entries to be blank rather than have null written

select * from identifier($match_off);
drop table identifier($match_off);

select * from PRODUCT_XC_OFFERBANK where sku < 1000; --one of these offers does not have a threshold amount
--so im using threshold is null and threshold is not null instead

-- matching to the offerbank and returning table for comms
--matching on points
create or replace temp table xc_barcodes as
select      a.sku, a.product_desc, a.offer_cell, a.points, a.pounds, a.threshold_amount, a.threshold_quantity, barcode, start_date
from        identifier($match_off) a
left join   product_xc_offerbank b
on          a.sku = b.sku
and         a.points = b.points
where a.THRESHOLD_AMOUNT is null and a.points is not null
union
--matching on pounds
select      a.sku, a.product_desc, a.offer_cell, a.points, a.pounds, a.threshold_amount, a.threshold_quantity, barcode, start_date
from        identifier($match_off) a
left join   product_xc_offerbank b
on          a.sku = b.sku
and         a.pounds = b.pounds
where a.THRESHOLD_AMOUNT is null and a.POUNDS is not null
union
--matching for threshold offers
select      a.sku, a.product_desc, a.offer_cell, a.points, a.pounds, a.threshold_amount, a.threshold_quantity, barcode, start_date
from        identifier($match_off) a
left join   product_xc_offerbank b
on          a.sku = b.sku
and         a.threshold_amount = b.THRESHOLD_AMOUNT
where a.THRESHOLD_AMOUNT is not null;

-- check over it
select * from xc_barcodes;


-- creating final table to send to comms
create or replace temp table xc_barcodes as
select          *,
case            when barcode is null then 'TO BE RAISED'
                else 'EXISTING' end as offer_status
from            xc_barcodes
order by offer_status desc
;

-- export as csv and return to comms
select * from xc_barcodes;
