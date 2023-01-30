USE ROLE RL_PROD_MARKETING_ANALYTICS;
USE DATABASE CUSTOMER_ANALYTICS;
USE WAREHOUSE WHS_PROD_MARKETING_ANALYTICS_LARGE;
USE SCHEMA PRODUCT_REPORTING;

--LOOK INTO SALES OF SKU LIST FOR NI
CREATE OR REPLACE TEMP TABLE NI_CUSTS AS
select a.*, b.postcode from custsource a
inner join EDWS_PROD.PROD_CMT_PRESENTATION.VW_CA_CUSTOMER_ACCOUNT b on a.sr_id=b.sr_id
where postcode like 'BT%';

select count (*) from NI_CUSTS; -- 127858
select * from NI_CUSTS;

create or replace temp table irish_skus_YB as
    select cust.sr_id,
           a.date_key
			,max(a.date_key)  as LAST_PURCHASE_DATE
			,count(distinct a.transaction_key) as NUM_TRANSACTIONS

		from adw_prod.INC_PL.transaction_dim as a
		    Inner join adw_prod.INC_PL.customer_dim as b
		            on a.customer_key = b.customer_key

		    Inner join EDWS_PROD.PROD_CMT_PRESENTATION.VW_CA_Customer_Account as c
		            on b.party_account_no = sha2(cast(c.party_account_no as varchar(50)), 256)
			inner join NI_CUSTS as cust
					on cust.sr_id=c.sr_id
		    inner join adw_prod.INC_PL.item_fact as d
		            on a.transaction_key = d.transaction_key
		    inner join "ADW_PROD"."INC_PL"."PRODUCT_DIM" as e
		            on e.product_key = d.product_key

        where d.extended_price > 0
			and a.date_key <= '2020-01-30' AND a.date_key >= '2019-01-30'
			            and e.valid_to_DT >= current_date
	and e.sku_no in (7965955,	8097729,	1308228,	1196757,	1174281,	8433,	6012710,	662789,	7436859,	7555367,	1092547,	8109372,	7555636,	131544,	1111323,	1246377,	7575577,	7230677,	1128291,	6825730,	7714684,	7714646,	678902,	7983393,	7700737,	7537774,	7537809,	7983431,	7466414,	7085499,	7996897,	3659274,	7512184,	7624516,	7714717,	439817)
		group by 1,2;


select min(date_key), max(date_key) from irish_skus_YB;

select count(distinct sr_id) from irish_skus_YB;
-- 57,642 - custs from the year before brexit


create or replace temp table irish_skus_YA as
    select cust.sr_id,
           a.date_key
			,max(a.date_key)  as LAST_PURCHASE_DATE
			,count(distinct a.transaction_key) as NUM_TRANSACTIONS

		from adw_prod.INC_PL.transaction_dim as a
		    Inner join adw_prod.INC_PL.customer_dim as b
		            on a.customer_key = b.customer_key

		    Inner join EDWS_PROD.PROD_CMT_PRESENTATION.VW_CA_Customer_Account as c
		            on b.party_account_no = sha2(cast(c.party_account_no as varchar(50)), 256)
			inner join NI_CUSTS as cust
					on cust.sr_id=c.sr_id
		    inner join adw_prod.INC_PL.item_fact as d
		            on a.transaction_key = d.transaction_key
		    inner join "ADW_PROD"."INC_PL"."PRODUCT_DIM" as e
		            on e.product_key = d.product_key

        where d.extended_price > 0
			and a.date_key >= '2020-01-31' AND a.date_key <= '2021-01-31'
			            and e.valid_to_DT >= current_date
	and e.sku_no in (7965955,	8097729,	1308228,	1196757,	1174281,	8433,	6012710,	662789,	7436859,	7555367,	1092547,	8109372,	7555636,	131544,	1111323,	1246377,	7575577,	7230677,	1128291,	6825730,	7714684,	7714646,	678902,	7983393,	7700737,	7537774,	7537809,	7983431,	7466414,	7085499,	7996897,	3659274,	7512184,	7624516,	7714717,	439817)
		group by 1,2;

select count(distinct sr_id) from irish_skus_YA;
-- 67,155 - custs from year brexit - a year after

create or replace temp table irish_skus_LY as
    select cust.sr_id,
           a.date_key
			,max(a.date_key)  as LAST_PURCHASE_DATE
			,count(distinct a.transaction_key) as NUM_TRANSACTIONS

		from adw_prod.INC_PL.transaction_dim as a
		    Inner join adw_prod.INC_PL.customer_dim as b
		            on a.customer_key = b.customer_key

		    Inner join EDWS_PROD.PROD_CMT_PRESENTATION.VW_CA_Customer_Account as c
		            on b.party_account_no = sha2(cast(c.party_account_no as varchar(50)), 256)
			inner join NI_CUSTS as cust
					on substring(cust.sr_id, 4,11)=c.party_account_no
		    inner join adw_prod.INC_PL.item_fact as d
		            on a.transaction_key = d.transaction_key
		    inner join "ADW_PROD"."INC_PL"."PRODUCT_DIM" as e
		            on e.product_key = d.product_key

        where d.extended_price > 0
			and a.date_key >= (current_date-365) and a.date_key <=current_date
			            and e.valid_to_DT >= current_date
	and e.sku_no in (7965955,	8097729,	1308228,	1196757,	1174281,	8433,	6012710,	662789,	7436859,	7555367,	1092547,	8109372,	7555636,	131544,	1111323,	1246377,	7575577,	7230677,	1128291,	6825730,	7714684,	7714646,	678902,	7983393,	7700737,	7537774,	7537809,	7983431,	7466414,	7085499,	7996897,	3659274,	7512184,	7624516,	7714717,	439817)
		group by 1,2;

select count(distinct sr_id) from irish_skus_LY;
-- 93,419- custs who bought into those SKUs last year


create or replace temp table irish_skus_6M as
    select cust.sr_id,
           a.date_key
			,max(a.date_key)  as LAST_PURCHASE_DATE
			,count(distinct a.transaction_key) as NUM_TRANSACTIONS

		from adw_prod.INC_PL.transaction_dim as a
		    Inner join adw_prod.INC_PL.customer_dim as b
		            on a.customer_key = b.customer_key

		    Inner join EDWS_PROD.PROD_CMT_PRESENTATION.VW_CA_Customer_Account as c
		            on b.party_account_no = sha2(cast(c.party_account_no as varchar(50)), 256)
			inner join NI_CUSTS as cust
					on substring(cust.sr_id, 4,11)=c.party_account_no
		    inner join adw_prod.INC_PL.item_fact as d
		            on a.transaction_key = d.transaction_key
		    inner join "ADW_PROD"."INC_PL"."PRODUCT_DIM" as e
		            on e.product_key = d.product_key

        where d.extended_price > 0
			and a.date_key >= (current_date-180) and a.date_key <=current_date
			            and e.valid_to_DT >= current_date
	and e.sku_no in (7965955,	8097729,	1308228,	1196757,	1174281,	8433,	6012710,	662789,	7436859,	7555367,	1092547,	8109372,	7555636,	131544,	1111323,	1246377,	7575577,	7230677,	1128291,	6825730,	7714684,	7714646,	678902,	7983393,	7700737,	7537774,	7537809,	7983431,	7466414,	7085499,	7996897,	3659274,	7512184,	7624516,	7714717,	439817)
		group by 1,2;

select count(distinct sr_id) from irish_skus_6M;
-- 74,402 - custs who bought into those SKUs in last 6m



-- custs that bought the year prior to brexit but not in the last year
SELECT COUNT(DISTINCT SR_ID) FROM irish_skus_YB WHERE SR_ID not in (Select distinct sr_id from irish_skus_LY); -- 7653

-- custs that bought the year prior to brexit but not in the last 6 months
SELECT COUNT(DISTINCT SR_ID) FROM irish_skus_YB WHERE SR_ID not in (Select distinct sr_id from irish_skus_6M); --14,915


select count(distinct sr_id) from irish_skus; --105759 OVER ALL TIME
select count(distinct sr_id) from irish_skus; --67544 BETWEEN 2019-10-20 AND 2020-04-20
select min(transaction_Date) from irish_skus; --2019-10-20 -- ONLY GOES AS FAR BACK AS 20-10-2019
-- SO LETS TRY PEOPLE WHO BOUGHT IN THE 6 MONTHS AFTER 2019-10-20

select * from EDWS_PROD.PROD_CMT_PRESENTATION.VW_CA_Customer_Account limit 5;
select * from adw_prod.INC_PL.transaction_dim order by date_key desc;; -- customer id
select * from adw_prod.INC_PL.item_fact;
select * from "ADW_PROD"."INC_PL"."PRODUCT_DIM" limit 5; where valid_to_DT >= current_date
select * from adw_prod.INC_PL.customer_dim limit 10; -- inferred customer ID and customer key

create or replace temp table irish_skus_LY as
    select sr_id, a.transaction_Date
         ,sum(EXTENDED_PRICE) as SUM_EXTENDED_PRICE
         ,max(transaction_date)  as LAST_PURCHASE_DATE
         ,count(distinct transaction_date) as NUM_TRANSACTIONS
         ,sum(item_quantity) as SUM_ITEM_QUANTITY_26WEEKS
--       ,sum(item_quantity*margin)/sum(item_quantity)  as avg_margin
         ,sum(case when transaction_date >= (current_date-84) then item_quantity else 0 end) as SUM_ITEM_QUANTITY_12WEEKS

      from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_SHOPPING_TRANSACTION_LINE as a
                  inner join NI_CUSTS cust on cust.sr_id=a.party_account_id
                  inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_EAN d on a.ean_key=d.ean_key
                  inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_SKU_MAP as e on e.sku_key = d.sku_key
                        inner join EDWS_PROD.PROD_CMT_PRESENTATION.vw_sku as sku on sku.sku_key = e.sku_key

      where a.item_refunded_ind = 'N'
         and a.extended_price > 0
         and a.transaction_date >= (current_date-365)  -- PEOPLE WHO BOUGHT IN THE LAST YEAR
            and sku.end_date is null
   and sku.sku in (7965955,   8097729,   1308228,   1196757,   1174281,   8433,  6012710,   662789,    7436859,   7555367,   1092547,   8109372,   7555636,   131544,    1111323,   1246377,   7575577,   7230677,   1128291,   6825730,   7714684,   7714646,   678902,    7983393,   7700737,   7537774,   7537809,   7983431,   7466414,   7085499,   7996897,   3659274,   7512184,   7624516,   7714717,   439817)

-- and sku.brand = 'OWN-LABEL'
      group by 1,2;

select count(distinct sr_id) from irish_skus_LY; --92662 in last year

create or replace temp table irish_skus_6M as
    select sr_id, a.transaction_Date
         ,sum(EXTENDED_PRICE) as SUM_EXTENDED_PRICE
         ,max(transaction_date)  as LAST_PURCHASE_DATE
         ,count(distinct transaction_date) as NUM_TRANSACTIONS
         ,sum(item_quantity) as SUM_ITEM_QUANTITY_26WEEKS
--       ,sum(item_quantity*margin)/sum(item_quantity)  as avg_margin
         ,sum(case when transaction_date >= (current_date-84) then item_quantity else 0 end) as SUM_ITEM_QUANTITY_12WEEKS

      from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_SHOPPING_TRANSACTION_LINE as a
                  inner join NI_CUSTS cust on cust.sr_id=a.party_account_id
                  inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_EAN d on a.ean_key=d.ean_key
                  inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_SKU_MAP as e on e.sku_key = d.sku_key
                        inner join EDWS_PROD.PROD_CMT_PRESENTATION.vw_sku as sku on sku.sku_key = e.sku_key

      where a.item_refunded_ind = 'N'
         and a.extended_price > 0
         and a.transaction_date >= (current_date-180)  -- PEOPLE WHO BOUGHT IN THE LAST 6M
            and sku.end_date is null
   and sku.sku in (7965955,   8097729,   1308228,   1196757,   1174281,   8433,  6012710,   662789,    7436859,   7555367,   1092547,   8109372,   7555636,   131544,    1111323,   1246377,   7575577,   7230677,   1128291,   6825730,   7714684,   7714646,   678902,    7983393,   7700737,   7537774,   7537809,   7983431,   7466414,   7085499,   7996897,   3659274,   7512184,   7624516,   7714717,   439817)


SELECT COUNT(DISTINCT SR_ID) FROM irish_skus WHERE SR_ID in (Select distinct sr_id from irish_skus_LY); --60338

SELECT COUNT(DISTINCT SR_ID) FROM irish_skus WHERE SR_ID not in (Select distinct sr_id from irish_skus_LY); --7206


--============================== KATES WAY ====================================
select * from EDWS_PROD.PROD_CMT_PRESENTATION.vw_sku_summary;
select min(transaction_date) from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_SHOPPING_TRANSACTION_LINE;

select distinct e.SKU, sku.sku_desc, sku.sub_category, sum.avg_selling_price_4weeks, margin_4weeks
    					from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_EAN d
						inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_SKU_MAP as e on e.sku_key = d.sku_key
                        inner join EDWS_PROD.PROD_CMT_PRESENTATION.vw_sku as sku on sku.sku_key = e.sku_key
    					inner join EDWS_PROD.PROD_CMT_PRESENTATION.vw_sku_summary sum on sku.sku = sum.sku
where sku.sku in (7965955,
8097729,
1308228,
1196757,
1174281,
8433,
6012710,
662789,
7436859,
1092547,
8109372,
7555636,
1111323,
1246377,
7575577,
7230677,
1128291,
6825730,
7714684,
7714646,
678902,
7983393,
7700737,
7537774,
7537809,
7983431,
7466414,
7996897,
3659274,
7512184,
7714717,
439817
);

--LOOK INTO SALES OF SKU LIST FOR NI
CREATE OR REPLACE TEMP TABLE NI_CUSTS AS
select a.*, b.postcode from custsource a
inner join EDWS_PROD.PROD_CMT_PRESENTATION.VW_CA_CUSTOMER_ACCOUNT b on a.sr_id=b.sr_id
where postcode like 'BT%'; --117079


create or replace temp table irish_skus as
    select sr_id, a.transaction_Date
			,sum(EXTENDED_PRICE) as SUM_EXTENDED_PRICE
			,max(transaction_date)  as LAST_PURCHASE_DATE
			,count(distinct transaction_date) as NUM_TRANSACTIONS
			,sum(item_quantity) as SUM_ITEM_QUANTITY_26WEEKS
--			,sum(item_quantity*margin)/sum(item_quantity)  as avg_margin
			,sum(case when transaction_date >= (current_date-84) then item_quantity else 0 end) as SUM_ITEM_QUANTITY_12WEEKS

		from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_SHOPPING_TRANSACTION_LINE as a
						inner join NI_CUSTS cust on cust.sr_id=a.party_account_id
						inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_EAN d on a.ean_key=d.ean_key
						inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_SKU_MAP as e on e.sku_key = d.sku_key
                        inner join EDWS_PROD.PROD_CMT_PRESENTATION.vw_sku as sku on sku.sku_key = e.sku_key

		where a.item_refunded_ind = 'N'
			and a.extended_price > 0
			and a.transaction_date >= (current_date-1460) AND TRANSACTION_DATE < '2020-04-20' --4 years ago - end of 2018 to get the whole of 'normal shopping' 2019
            and sku.end_date is null
	and sku.sku in (7965955,	8097729,	1308228,	1196757,	1174281,	8433,	6012710,	662789,	7436859,	7555367,	1092547,	8109372,	7555636,	131544,	1111323,	1246377,	7575577,	7230677,	1128291,	6825730,	7714684,	7714646,	678902,	7983393,	7700737,	7537774,	7537809,	7983431,	7466414,	7085499,	7996897,	3659274,	7512184,	7624516,	7714717,	439817)

--	and sku.brand = 'OWN-LABEL'
		group by 1,2;

select count(distinct sr_id) from irish_skus; --105759 OVER ALL TIME
select count(distinct sr_id) from irish_skus; --67544 BETWEEN 2019-10-20 AND 2020-04-20
select min(transaction_Date) from irish_skus; --2019-10-20 -- ONLY GOES AS FAR BACK AS 20-10-2019
-- SO LETS TRY PEOPLE WHO BOUGHT IN THE 6 MONTHS AFTER 2019-10-20

select * from adw_prod.INC_PL.transaction_dim; -- customer id

select * from adw_prod.INC_PL.customer_dim; -- inferred customer ID and customer key

create or replace temp table irish_skus_LY as
    select sr_id, a.transaction_Date
         ,sum(EXTENDED_PRICE) as SUM_EXTENDED_PRICE
         ,max(transaction_date)  as LAST_PURCHASE_DATE
         ,count(distinct transaction_date) as NUM_TRANSACTIONS
         ,sum(item_quantity) as SUM_ITEM_QUANTITY_26WEEKS
--       ,sum(item_quantity*margin)/sum(item_quantity)  as avg_margin
         ,sum(case when transaction_date >= (current_date-84) then item_quantity else 0 end) as SUM_ITEM_QUANTITY_12WEEKS

      from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_SHOPPING_TRANSACTION_LINE as a
                  inner join NI_CUSTS cust on cust.sr_id=a.party_account_id
                  inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_EAN d on a.ean_key=d.ean_key
                  inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_SKU_MAP as e on e.sku_key = d.sku_key
                        inner join EDWS_PROD.PROD_CMT_PRESENTATION.vw_sku as sku on sku.sku_key = e.sku_key

      where a.item_refunded_ind = 'N'
         and a.extended_price > 0
         and a.transaction_date >= (current_date-365)  -- PEOPLE WHO BOUGHT IN THE LAST YEAR
            and sku.end_date is null
   and sku.sku in (7965955,   8097729,   1308228,   1196757,   1174281,   8433,  6012710,   662789,    7436859,   7555367,   1092547,   8109372,   7555636,   131544,    1111323,   1246377,   7575577,   7230677,   1128291,   6825730,   7714684,   7714646,   678902,    7983393,   7700737,   7537774,   7537809,   7983431,   7466414,   7085499,   7996897,   3659274,   7512184,   7624516,   7714717,   439817)

-- and sku.brand = 'OWN-LABEL'
      group by 1,2;

select count(distinct sr_id) from irish_skus_LY; --92662 in last year

create or replace temp table irish_skus_6M as
    select sr_id, a.transaction_Date
         ,sum(EXTENDED_PRICE) as SUM_EXTENDED_PRICE
         ,max(transaction_date)  as LAST_PURCHASE_DATE
         ,count(distinct transaction_date) as NUM_TRANSACTIONS
         ,sum(item_quantity) as SUM_ITEM_QUANTITY_26WEEKS
--       ,sum(item_quantity*margin)/sum(item_quantity)  as avg_margin
         ,sum(case when transaction_date >= (current_date-84) then item_quantity else 0 end) as SUM_ITEM_QUANTITY_12WEEKS

      from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_SHOPPING_TRANSACTION_LINE as a
                  inner join NI_CUSTS cust on cust.sr_id=a.party_account_id
                  inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_EAN d on a.ean_key=d.ean_key
                  inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_SKU_MAP as e on e.sku_key = d.sku_key
                        inner join EDWS_PROD.PROD_CMT_PRESENTATION.vw_sku as sku on sku.sku_key = e.sku_key

      where a.item_refunded_ind = 'N'
         and a.extended_price > 0
         and a.transaction_date >= (current_date-180)  -- PEOPLE WHO BOUGHT IN THE LAST 6M
            and sku.end_date is null
   and sku.sku in (7965955,   8097729,   1308228,   1196757,   1174281,   8433,  6012710,   662789,    7436859,   7555367,   1092547,   8109372,   7555636,   131544,    1111323,   1246377,   7575577,   7230677,   1128291,   6825730,   7714684,   7714646,   678902,    7983393,   7700737,   7537774,   7537809,   7983431,   7466414,   7085499,   7996897,   3659274,   7512184,   7624516,   7714717,   439817)


SELECT COUNT(DISTINCT SR_ID) FROM irish_skus WHERE SR_ID in (Select distinct sr_id from irish_skus_LY); --60338

SELECT COUNT(DISTINCT SR_ID) FROM irish_skus WHERE SR_ID not in (Select distinct sr_id from irish_skus_LY); --7206
