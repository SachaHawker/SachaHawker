/* change input table*/
create or replace temp table base as
select * from
(select sr_id, sku1 as sku, barcode1 as barcode, bpoints1 as points,  ocid1 as ocid , threshold1 as threshold, external_id1 as bl_number from EDWS_PROD.PROD_CMT_CAMPAIGN_01.XC_AUTO_CustSku
union all select sr_id, sku2 as sku, barcode2 as barcode, bpoints2 as points, ocid2 as ocid, threshold2 as threshold, external_id2 as bl_number  from EDWS_PROD.PROD_CMT_CAMPAIGN_01.XC_AUTO_CustSku
union all select sr_id, sku3 as sku, barcode3 as barcode, bpoints3 as points, ocid3 as ocid, threshold3 as threshold, external_id3 as bl_number   from EDWS_PROD.PROD_CMT_CAMPAIGN_01.XC_AUTO_CustSku
union all select sr_id, sku4 as sku, barcode4 as barcode, bpoints4 as points, ocid4 as ocid, threshold4 as threshold, external_id4 as bl_number    from EDWS_PROD.PROD_CMT_CAMPAIGN_01.XC_AUTO_CustSku
union all select sr_id, sku5 as sku, barcode5 as barcode, bpoints5 as points, ocid5 as ocid, threshold5 as threshold, external_id5 as bl_number   from EDWS_PROD.PROD_CMT_CAMPAIGN_01.XC_AUTO_CustSku
union all select sr_id, sku6 as sku, barcode6 as barcode, bpoints6 as points, ocid6 as ocid, threshold6 as threshold, external_id6 as bl_number   from EDWS_PROD.PROD_CMT_CAMPAIGN_01.XC_AUTO_CustSku
union all select sr_id, sku7 as sku, barcode7 as barcode, bpoints7 as points, ocid7 as ocid, threshold7 as threshold, external_id7 as bl_number  from EDWS_PROD.PROD_CMT_CAMPAIGN_01.XC_AUTO_CustSku
union all select sr_id, sku8 as sku, barcode8 as barcode, bpoints8 as points, ocid8 as ocid, threshold8 as threshold, external_id8 as bl_number   from EDWS_PROD.PROD_CMT_CAMPAIGN_01.XC_AUTO_CustSku
union all select sr_id, sku9 as sku, barcode9 as barcode, bpoints9 as points, ocid9 as ocid, threshold9 as threshold, external_id9 as bl_number  from EDWS_PROD.PROD_CMT_CAMPAIGN_01.XC_AUTO_CustSku
union all select sr_id, sku10 as sku, barcode10 as barcode, bpoints10 as points, ocid10 as ocid, threshold10 as threshold, external_id10 as bl_number   from EDWS_PROD.PROD_CMT_CAMPAIGN_01.XC_AUTO_CustSku    )
where sku is not null;

/*Check if thresholds are GOLD yes*/
select distinct sku, b.INSTORE, B.GOL,
                b.coupon_id as couponid
from base a
    inner join EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_COUPON_DETAILS b
    on a.barcode=b.barcode where sku < 1000
and GOL = 'Yes';





select * from EDWS_PROD.PROD_CMT_CAMPAIGN_01.XC_AUTO_CustSku limit 5;

select distinct b.INSTORE, B.GOL,b.coupon_id, b.coupon_name, b.barcode
from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_COUPON_DETAILS b
 where coupon_name like '2324_XC2%';
-- and barcode in (280010022140763,
-- 280014022140778,
-- 280016022140789,
-- 280010022140794,
-- 280014022140808,
-- 280022022140810,
-- 280028022140829,
-- 280010022140831,
-- 280012022140842,
-- 280016022140857
-- )

select * from EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_COUPON_DETAILS limit 5;
--