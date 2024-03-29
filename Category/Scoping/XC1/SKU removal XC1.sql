--remove from:
--1. SH_X01232_SKU_LIST
--2. SH_X01232_BARCODE_POINTS
--3. SH_X01232_ADNUM



/*create or replace table SH_X01232_SKU_LIST as
    select * from SH_X01232_SKU_LIST
             where sku not in
                   (7714646, 7466414,
                    7996897, 1174281,
                    7537774, 7714684,
                    7714717, 439817,
                    1246377, 7700737,
                    7537809, 3659274,
                    678902, 6825730,
                    7983393, 7965955,
                    7983431, 1128291,
                    1111323, 7230677,
                    1308228, 8097729,
                    7436859, 7555636,
                    7512184, 8433, 8109372);*/

-- SKU LIST CHECK --
select * from SH_X01232_SKU_LIST
             where sku in
                   (7714646, 7466414,
                    7996897, 1174281,
                    7537774, 7714684,
                    7714717, 439817,
                    1246377, 7700737,
                    7537809, 3659274,
                    678902, 6825730,
                    7983393, 7965955,
                    7983431, 1128291,
                    1111323, 7230677,
                    1308228, 8097729,
                    7436859, 7555636,
                    7512184, 8433, 8109372);

select * from SH_X01232_SKU_LIST
             where sku in
                   (242);


-- BARCODE POINTS --
/*create or replace table SH_X01232_BARCODE_POINTS as
    select * from SH_X01232_BARCODE_POINTS
             where sku not in
                   (7714646, 7466414,
                    7996897, 1174281,
                    7537774, 7714684,
                    7714717, 439817,
                    1246377, 7700737,
                    7537809, 3659274,
                    678902, 6825730,
                    7983393, 7965955,
                    7983431, 1128291,
                    1111323, 7230677,
                    1308228, 8097729,
                    7436859, 7555636,
                    7512184, 8433, 8109372);*/

-- BARCODE CHECK
    select * from SH_X01232_BARCODE_POINTS
             where sku in
                   (7714646, 7466414,
                    7996897, 1174281,
                    7537774, 7714684,
                    7714717, 439817,
                    1246377, 7700737,
                    7537809, 3659274,
                    678902, 6825730,
                    7983393, 7965955,
                    7983431, 1128291,
                    1111323, 7230677,
                    1308228, 8097729,
                    7436859, 7555636,
                    7512184, 8433, 8109372);

-- ADNUM
/*create or replace table SH_X01232_ADNUM as
    select * from SH_X01232_ADNUM
             where sku not in
                   (7714646, 7466414,
                    7996897, 1174281,
                    7537774, 7714684,
                    7714717, 439817,
                    1246377, 7700737,
                    7537809, 3659274,
                    678902, 6825730,
                    7983393, 7965955,
                    7983431, 1128291,
                    1111323, 7230677,
                    1308228, 8097729,
                    7436859, 7555636,
                    7512184, 8433, 8109372);*/

    select * from SH_X01232_ADNUM
             where sku in
                   (7714646, 7466414,
                    7996897, 1174281,
                    7537774, 7714684,
                    7714717, 439817,
                    1246377, 7700737,
                    7537809, 3659274,
                    678902, 6825730,
                    7983393, 7965955,
                    7983431, 1128291,
                    1111323, 7230677,
                    1308228, 8097729,
                    7436859, 7555636,
                    7512184, 8433, 8109372);

/*

update SH_X01232_BARCODE_POINTS
set points = 100
where sku = 250;

update SH_X01232_BARCODE_POINTS
set points = 70
where sku = 251;

select sku, points from SH_X01232_BARCODE_POINTS
where sku in (251, 250);*/
