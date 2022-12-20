USE ROLE RL_PROD_MARKETING_ANALYTICS;
USE DATABASE CUSTOMER_ANALYTICS;
USE WAREHOUSE WHS_PROD_MARKETING_ANALYTICS_XSMALL;
USE SCHEMA TD_REPORTING;


select count(*), threshold from TD09_2223_Petrol_DM where target_control_flag=1 group by 2;

select * from CUSTOMER_ANALYTICS.PRODUCT_REPORTING.PRODUCT_PCA_REPOSITORY;

select * from PRODUCT_PPT2_PCA_REPOSITORY_ALL_RESULTS;

select * from "CUSTOMER_ANALYTICS"."PRODUCT_REPORTING"."PRODUCT_PPT3_PCA_REPOSITORY_ALL_RESULTS";

select * from "CUSTOMER_ANALYTICS"."PRODUCT_REPORTING"."PRODUCT_PPT4_PCA_REPOSITORY_ALL_RESULTS";

desc table PRODUCT_PPT2_PCA_REPOSITORY_ALL_RESULTS;
select * from "CUSTOMER_ANALYTICS"."PRODUCT_REPORTING"."PRODUCT_PPT2_PCA_REPOSITORY";

select * from "CUSTOMER_ANALYTICS"."PRODUCT_REPORTING"."PRODUCT_PPT2_PCA_REPOSITORY"
where end_date is null
    and campaign in (X06222)

select * from "CUSTOMER_ANALYTICS"."PRODUCT_REPORTING"."PRODUCT_PPT2_PCA_REPOSITORY"
         where campaign = 'X07222' limit 5;

select a.campaign,
       a.SKU,
       a.JS_ROI,
       a.prints,
       b.sku_desc,
       a.sales_uplift,
       a.cust_group,
       (a.redemption_cost+a.print_costs+a.valassis_cost) as cost,
       avg(case when c.avg_selling_price_12weeks>0 then c.avg_selling_price_12weeks else null end) as asp,
       avg(case when c.margin_12weeks>0 then c.margin_12weeks else null end) as margin
from "CUSTOMER_ANALYTICS"."PRODUCT_REPORTING"."PRODUCT_PPT2_PCA_REPOSITORY" a
left join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_C_SKU_MAP b
        on a.SKU= b.SKU
inner join EDWS_PROD.PROD_CMT_PRESENTATION.VW_SKU_SUMMARY c
        on b.sku_key = c.sku_key
    where b.end_date is null
    and a.campaign in ('X08222', 'X07222', 'X06222', 'X05222')
    and a.channel = 'Both'
      and cust_group = 'All'
    and a.prints >2000
    and a.sku in (276,
289,
295,
296,
299,
303,
319,
320,
330,
332,
6439,
457149,
670180,
1019209,
6005026,
6298031,
6318944,
6554736,
6615796,
6731645,
7264096,
7522551,
7598535,
7862200,
7862433,
7924805,
264,
284,
473,
283311,
349529,
1082388,
1304466,
6259624,
7778405,
7843573,
7861754,
7882440,
7930286,
7947566,
7998905,
8001352,
8017012,
261,
431,
553179,
1081459,
6577504,
6658685,
7718802,
7843571,
7853977,
8049885)
    group by 1,2,3,4,5,6,7,8
         order by SKU desc;




select SKU, SKU_DESC from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_C_SKU_MAP where SKU in (276,
289,
295,
296,
299,
303,
319,
320,
330,
332,
6439,
457149,
670180,
1019209,
6005026,
6298031,
6318944,
6554736,
6615796,
6731645,
7264096,
7522551,
7598535,
7862200,
7862433,
7924805,
264,
284,
473,
283311,
349529,
1082388,
1304466,
6259624,
7778405,
7843573,
7861754,
7882440,
7930286,
7947566,
7998905,
8001352,
8017012,
261,
431,
553179,
1081459,
6577504,
6658685,
7718802,
7843571,
7853977,
8049885

)

select distinct a.sku
                ,a.SKU_DESC
--                 ,SUB_CATEGORY
                ,avg(case when b.avg_selling_price_12weeks>0 then b.avg_selling_price_12weeks else null end) as asp
                ,avg(case when b.margin_12weeks>0 then b.margin_12weeks else null end) as margin
    from EDWS_PROD.PROD_CMT_PRESENTATION.VW_SKU a
    inner join EDWS_PROD.PROD_CMT_PRESENTATION.VW_SKU_SUMMARY b
        on a.sku_key = b.sku_key
    where a.end_date is null
    and a.SKU in (276,
289,
295,
296,
299,
303,
319,
320,
330,
332,
6439,
457149,
670180,
1019209,
6005026,
6298031,
6318944,
6554736,
6615796,
6731645,
7264096,
7522551,
7598535,
7862200,
7862433,
7924805,
264,
284,
473,
283311,
349529,
1082388,
1304466,
6259624,
7778405,
7843573,
7861754,
7882440,
7930286,
7947566,
7998905,
8001352,
8017012,
261,
431,
553179,
1081459,
6577504,
6658685,
7718802,
7843571,
7853977,
8049885)
group by 1,2;
