USE ROLE RL_PROD_MARKETING_ANALYTICS;
USE WAREHOUSE WHS_PROD_MARKETING_ANALYTICS_LARGE;
USE DATABASE CUSTOMER_ANALYTICS;
USE SCHEMA TD_REPORTING;

-- data imported
select * from uk_postcodes_long_lat;

-- add postal area to improve efficiency
create or replace temp table test1 as
    select postcode,
           position(' ' in postcode) as position_of_space,
           left(postcode, position_of_space-1) as postal_area,
           left(postcode, position_of_space+1) as postal_area2,
           left(postcode, position_of_space+2) as postal_area3,
           latitude,
           longitude
from UK_POSTCODES_LONG_LAT
;

-- save to permanent table
create or replace table UK_POSTCODES_LONG_LAT_AREA as
    select postcode,
           postal_area,
           postal_area2, -- most granular postal area we can use before calculation becomes too expensive
           postal_area3,
           latitude,
           longitude
from test1
;

-- output of store_distance.ipynb
select distinct len(postal_area2) from pfs_postcode_within_8km;

-- join to customers
create or replace temp table customer_pfs_closeness_flag_prelim as
select a.*,
       b.store_cd,
       b.store_name,
       b.postcode_store,
       b.distance

from (select ec_id,
             sr_id,
             full_nectar_card_num,
             postcode,
             case when position(' ' in postcode) <> 0 then upper(postcode)
                  when position(' ' in postcode) = 0 then
                      case when len(postcode) = 7 then upper(concat(left(postcode, 4), ' ', right(postcode, 3)))
                           when len(postcode) = 6 then upper(concat(left(postcode, 3), ' ', right(postcode, 3)))
                           when len(postcode) = 5 then upper(concat(left(postcode, 2), ' ', right(postcode, 3)))
                          end
                 end as correct_postcode, -- add spaces, capitalise, remain null if not long enough to be a real postcode
             position(' ' in correct_postcode) as position_of_space,
             left(correct_postcode, position_of_space+1) as postal_area2

      from EDWS_PROD.PROD_CMT_PRESENTATION.VW_CA_CUSTOMER_ACCOUNT
     ) as a
         left join pfs_postcode_within_8km as b
                   on a.postal_area2 = b.postal_area2
;

select * from customer_pfs_closeness_flag_prelim where postcode is not null order by ec_id, distance;

-- summarise, flag if customer does or does not have a close pfs store, get name of closest
create or replace table customer_pfs_closeness_flag as
select distinct ec_id,
                sr_id,
                full_nectar_card_num,
                postcode,
                iff(store_name is not null, 1, 0) as pfs_8km_flag,
                store_cd,
                store_name,
                postcode_store,
                distance
from (select ec_id,
             sr_id,
             full_nectar_card_num,
             correct_postcode as postcode,
             store_cd,
             store_name,
             postcode_store,
             distance,
             rank() over (partition by ec_id order by distance) as rank_distance
      from customer_pfs_closeness_flag_prelim
     )
where rank_distance = 1
;

select pfs_8km_flag, count(*), count(distinct ec_id) from customer_pfs_closeness_flag group by 1;

select * from customer_pfs_closeness_flag
where ec_id in (select distinct ec_id from (select ec_id, count(*) as cnt_row from customer_pfs_closeness_flag group by 1) where cnt_row > 1)
order by ec_id
;

select * from customer_pfs_closeness_flag where postcode is not null order by ec_id, distance;

select distinct store_name from customer_pfs_closeness_flag;

select * from customer_pfs_closeness_flag where postcode is not null and store_name is null;