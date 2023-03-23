use role RL_PROD_MARKETING_ANALYTICS;
use database CUSTOMER_ANALYTICS;
use warehouse WHS_PROD_MARKETING_ANALYTICS_X2LARGE;
use schema PRODUCT_REPORTING;

/*Can we do a threshold offer on fresh chicken –
what sub cats would this be, and are the margins/volumes going to be ok?*/




/*Can we do a threshold offer on sub-cats related to summery missions like picnic (snacking?) and BBQ? */

    select SKU, sku_desc  from EDWS_PROD.PROD_CMT_PRESENTATION.vw_sku where sku_desc like '%picnic%';
EDWS_PROD.PROD_EDW_SAS_ADHOC_VIEWS.VW_SKU_MAP

    -- skus:
    /* pork pie
    cocktail sausages
    coleslaw

     */
