# -*- coding: utf-8 -*-
"""
Created on Wed Jun 23 19:52:17 2021

@author: Marcelo
"""

import pandas as pd
import numpy as np
import datetime
import requests
import json
from pyrebase import pyrebase

#loop con uso de API
#def fire_to_gcp1(event,context):
config = {
    "apiKey": "AIzaSyB1zAAaFCJDcMMqPZIThkqp-rUmN-HOMqs",
    "authDomain": "vacanalytics-4b801.firebaseapp.com",
    "databaseURL": "https://vacanalytics-4b801.firebaseio.com",
    "projectId": "vacanalytics-4b801",
    "storageBucket": "vacanalytics-4b801.appspot.com",
    "messagingSenderId": "1085788461506",
    "appId": "1:1085788461506:web:15363d3c1552f7101e9e5b",
    "measurementId": "G-5L1P3NJZZ8"
}

firebase_ap = pyrebase.initialize_app(config)
db = firebase_ap.database()  

hist_db1 = db.child("iot").child("hist").child("Station1").get().val()
hist_db2 = db.child("iot").child("hist").child("Station2").get().val()


############################# process JSON data ##############################################3


#dictionary to dataframe
df_sta1 = pd.DataFrame.from_dict(hist_db1, orient='index')
df_sta2 = pd.DataFrame.from_dict(hist_db2, orient='index')

#format dates
df_sta1["DateTime"] = pd.to_datetime(df_sta1["DateTime"], format='%d-%m-%Y %H:%M:%S') #datetime.datetime.strptime
df_sta1.index = pd.to_datetime(df_sta1.index, format='%d-%m-%Y %H:%M:%S')
df_sta2["Date_Time"] = pd.to_datetime(df_sta2["Date_Time"], format='%d-%m-%Y %H:%M:%S') #datetime.datetime.strptime
df_sta2.index = pd.to_datetime(df_sta2.index, format='%d-%m-%Y %H:%M:%S')

#function to mode, with nice name
def mode_text(x):
    try:
        res = pd.Series.mode(x)[0]
    except:
        res = pd.Series.mode(x)
    return res
mode_text.__name__ = 'Moda'

#group by using resample at 'D' day level
df_res1 = df_sta1.set_index('DateTime').resample('D').agg({'DHTHumidity':['mean','max','min','std'], 
                        'DHTTemp':['mean','max','min'], 
                        'HumGnd':'mean', 
                        'Rainmm': 'sum',
                        'ThermoCouple':['mean','max','min'],
                        #'WindDir':lambda x: pd.Series.mode(x)[0], #pd.Series.mode,#returns multiple in case 2 or more equal
                        'WindDir': mode_text,
                        'WindSpeed':['mean','max'],
                        'ds18b20_cap':['mean','max','min','count']})
#simplify columns names                         
df_res1.columns = ['_'.join(col).strip() for col in df_res1.columns.values]

#def mode_text2(x):
#    return pd.Series.mode(x)
#mode_text.__name__ = 'Moda'
#group by using resample at 'D' day level
df_res2 = df_sta2.set_index('Date_Time').resample('D').agg({'DHT_Humidity':['mean','max','min','std'], 
                        'DHT_Temp':['mean','max','min'], 
                        'Hum_Gnd':'mean', 
                        'Rain_mm': 'sum',
                        'Thermo_Couple':['mean','max','min'],
                        #'WindDir':lambda x: pd.Series.mode(x)[0], #pd.Series.mode,#returns multiple in case 2 or more equal
                        'Wind_Dir': mode_text,
                        'Wind_Speed':['mean','max'],
                        'DS18b20_cap':['mean','max','min','count'],
                        'Solar_Volt':['mean','max','min','std'],
                        'Sunlight':['mean','max','min','std']})
#simplify columns names                         
df_res2.columns = ['_'.join(col).strip() for col in df_res2.columns.values]

#standardize column names
df_res1.columns = df_res2.columns[:19]
#key ID values
df_res1['ID_FINCA'] = 1
df_res1['ID_CLIENTE'] = 1
df_res2['ID_FINCA'] = 2
df_res2['ID_CLIENTE'] = 1

data_all = df_res1.append(df_res2, sort=False)

data_all['FECHA_HORA'] = data_all.index.date.astype(str)
#data_all['FECHA'] = data_all.index.date#['Date_Time'].date()
#data_all.dtypes
data_all = data_all.rename(columns={'DS18b20_cap_count': 'Count_Report'})

############################################################
# For automation of only 1 record per day per station

#last day only data
hoy = datetime.datetime.today() 
hoy = str(hoy.date())
ayer = datetime.datetime.today() - datetime.timedelta(days=1)
ayer = str(ayer.date())
df_sta_ayer = data_all.loc[data_all['FECHA_HORA'] == ayer]

#dictio = data_all.iloc[0,:].dropna().to_dict()


###############################################################
#send data to API using Requests package

#encoder de Numpy a objetos compatibles con JSON
class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(NpEncoder, self).default(obj)

#uso de la API una vez
url = 'https://data-science-proj-280908.ue.r.appspot.com/Meteo/'
headers = {'accept':'application/json','Content-Type':'application/json'}
for n in range(0,len(df_sta_ayer)):
    print(n)
    dictio = df_sta_ayer.iloc[n,:].dropna().to_dict()
    json_data = json.dumps(dictio, cls=NpEncoder)
    requests.post(url, headers=headers, data=json_data)
    
#return print("success")

