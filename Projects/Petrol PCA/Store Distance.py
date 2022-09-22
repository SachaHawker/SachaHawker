
import os
import getpass
import pandas as pd
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import snowflake.connector
import tempfile
import numpy as np


os.environ['http_proxy'] = 'http://a-proxy-p.bc.jsplc.net:8080'
os.environ['https_proxy'] = 'https://a-proxy-p.bc.jsplc.net:8080'
snowflake_conn = snowflake.connector.connect(
    user=os.environ.get('snowflake_uid'),
    password=os.environ.get('snowflake_pw'),
    account="sainsburys.eu-west-1",
    role="ANALYTICS_DB_POC_DEVELOPER",
    warehouse="ADW_MEDIUM_ADHOC_WH",
)

def haversine_np(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)

    All args must be of equal length.

    """
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2

    c = 2 * np.arcsin(np.sqrt(a))
    km = 6367 * c
    return km

query = "select store_cd,store_name,store_type_finance_desc,postcode,latitude,LONGITUDE from ADW_PROD.ADW_PROPERTY_PL.DIM_STORE"
store_data = pd.read_sql_query(query,snowflake_conn)
store_data = store_data[(store_data.STORE_TYPE_FINANCE_DESC=='Convenience')
                        | (store_data.STORE_TYPE_FINANCE_DESC=='Supermarket')]
store_data = store_data.dropna()
supermarkets = store_data[store_data.STORE_TYPE_FINANCE_DESC=='Supermarket']
convenience  = store_data[store_data.STORE_TYPE_FINANCE_DESC=='Convenience']
cols = list(convenience.columns)
cols = [i+"_CONV" for i in cols]
convenience.columns = cols
final_data = supermarkets.merge(convenience, how='cross')
final_data["distance"] = haversine_np(final_data.LATITUDE,
                                      final_data.LONGITUDE,
                                      final_data.LATITUDE_CONV,
                                      final_data.LONGITUDE_CONV)
final_data[final_data['distance']<10].to_clipboard()