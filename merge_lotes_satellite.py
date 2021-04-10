# -*- coding: utf-8 -*-
"""
Created on Fri Aug 14 13:55:17 2020
@author: Marcelo
"""

'''
0- load packages, define initial variables
'''
import os
#import numpy as np
import pandas as pd
#import matplotlib.pyplot as plt
#import datetime
#import time
from pathlib import Path
from myfunctions import API_usage
from myfunctions import GCP_Functions
from myfunctions import Data_Process
from myfunctions.tools import Tools
import argparse

ap = argparse.ArgumentParser()
ap.add_argument("-i", "--date_ini", type=str, default='no',
	help="define initial date yyyy-mm-dd")
ap.add_argument("-f", "--date_fin", type=str, default='no',
	help="define final date yyyy-mm-dd")
args = vars(ap.parse_args())

Date_Ini = (args["date_ini"]) 
Date_Fin = (args["date_fin"]) 
#Date_Ini= '20190101'
#Date_Fin= datetime.date.today().strftime("%Y%m%d")

bands=['_bm','_lai','_cp','ndf','_ndvi','_ndwi'] #selected bands

'''
1- Function for login to API, store token and get Clientes, lotes
'''
token = API_usage.login_api()
clientes = API_usage.get_clientes(token)
lotes = API_usage.get_lotes(token)

#leer ultimas filas de lotes_variables del cliente
'''
si hay datos, date_ini es la mayor fecha de la tabla SQL, sino, date_ini es la menor de las imagenes
bajar data.json de bucket, al final hacer merge y subir a GCP
bajar bandas_semnanal.csv, al final hacer merge y subir a GCP 
'''

'''
1-b loop throught clientes and read lotes
'''
clientes = clientes[0:1]
#define user
for client in clientes:
    print (client)
    cliente = 'ID_CLIENTE-'+str(client)
    analysis_area = '../db_'+cliente+'/'


#start = time.time() 

'''
2- read and join data
'''
#for cloud files
bucket_files = 'satellite_storage'
folder ='Data/Database/'
pref = 'resumen_vertical_lotes_medidas'
dest= '../Data/Database_ready/'
#listar de Cloud storage
objetos = list(GCP_Functions.list_all_blobs(bucket_files,prefix=folder+cliente+'/'+pref,delimiter='/')) #list fro cloud storage
destination = dest + cliente 
Path(destination).mkdir(parents=True, exist_ok=True) #create folder
for n in objetos:
    if len(n.split('/')[-1])>0:
        GCP_Functions.download_blob(bucket_files, n, destination +'/'+ n.split('/')[-1]) #dowloand fro GCP storage

#listr objetos locales en disco
prefixed = [filename for filename in os.listdir(destination) if filename.startswith(pref)]
#leer y combinar archivos
combined_csv_data = pd.concat([pd.read_csv(destination+'/'+f, delimiter=',', encoding='UTF-8') for f in prefixed])
combined_csv_data = combined_csv_data.sort_values(by=['name','band','date'])
combined_csv_data = combined_csv_data.dropna()
#to correct lotes_names Juan Camilo Camacho // pasar correccion al sat_processing.
combined_csv_data['name_c'] = combined_csv_data['name'].apply(lambda x: Tools.corr_num(x))
combined_csv_data.drop(['name', 'poly'], axis=1, inplace=True)
combined_csv_data['band'] = combined_csv_data['band'].apply(lambda x: x.lower()) #to lower cases
#join with lotes_id
combined_csv_data = combined_csv_data.merge(lotes, how='left', left_on=['name_c'], right_on=['nombre_lote'])
combined_csv_data.drop(columns=['nombre_lote'], inplace=True)
#format date
combined_csv_data['date'] = pd.to_datetime(combined_csv_data['date'], format='%Y%m%d')
#read file, format date and reorder dataframe
data = combined_csv_data.sort_values(by=['lote_id','band','date']) #order dataframe
data = data.loc[:,['lote_id','name_c','band','date','mean_value','sum_value','max_value','std_dev','count_pxl','perc_90','x','y']]
data.reset_index(drop = True, inplace=True)
data.drop_duplicates(keep=False,inplace=True)


'''
3- processing of data creating continued dates dataframe
'''
#create dataframe of lote_id, name, band and min/max date
data_ind = Data_Process.Base_data(data, bands, Date_Ini, destination)

#guardar en DB SQL estructurada Lotes_variables

'''
3b- move blobs to archive
'''
for n in objetos:
    if len(n.split('/')[-1])>0:
        new_name = n.replace("/Database/", "/Archive_DB/")
        GCP_Functions.copy_blob(bucket_files, n, bucket_files, new_name)
        GCP_Functions.delete_blob(bucket_files, n)

for file in prefixed:
    print(destination+'/'+file)
    os.remove(destination+'/'+file)
    
'''
3c- upload new files to bucket (json)
'''
processed_files = 'Data/Processed/'+cliente+'/'
data_ready = [filename for filename in os.listdir(destination)]
for n in data_ready:
    #new_name = n.replace("/Database/", "/Archive_DB/")
    GCP_Functions.upload_blob(bucket_files, destination+'/'+n, processed_files+n)
    




##############################################################################################################
#################### LAGS DE SATELITE
#group by multiple index
data_ind2 = data_ind.set_index(["lote_id",'name_c','band','date'])
data_ind2.drop(columns=['sum_value','max_value','count_pxl','x','y'], inplace=True) #remove unused variables

cols = data_ind2.columns.tolist()
data_ind3 = data_ind2.copy()
for n in range(1,5+1):
    shifted = data_ind2.groupby(level=["lote_id","band"]).shift(n)
    data_ind3 = data_ind3.join(shifted.rename(columns=lambda x: x +"_lag"+str(n)))

# simplificar indices
data_ind3.reset_index(drop = False, inplace=True)
#partir en diferentes dataframes y combinar horizontalmente, con suffix de la banda
'''
#aqui esta el problema de RAM
#para que uso esto?

bands_lags = pd.DataFrame()
for b in bands:
    keep_same = {'lote_id', 'name_c','band','date'}
    frame = data_ind3.loc[data_ind3['band']==b]
    frame.columns = ['{}{}'.format(c, '' if c in keep_same else str(b)) for c in frame.columns]
    frame.drop(columns=['band'], inplace=True) 
    if bands_lags.empty:
        bands_lags = frame
    else:
        bands_lags = bands_lags.merge(frame, how='left', left_on=['lote_id','name_c','date'], right_on=['lote_id','name_c','date'])

bands_lags.to_csv (destination+'/bands_histo.csv', index = True, header=True)

del(bands_lags)    
'''    
#################### JOIN ALL - seguimiento hatos, imagenes satelitales, analisis suelos
#bandas poner al mismo nivel horizontal por fecha/lote
#remover fechas otras, actividad y producto
'''
full_seguimiento2 = full_seguimiento.copy()
full_seguimiento2.drop(columns=['ACTIVIDAD','PRODUCTO'], inplace=True) #'ACTIVIDAD'

seguimiento_lag_inds = bands_lags.merge(full_seguimiento2, how='left', left_on=['lote_id','name_c','date'], right_on=['lote_id','name_c','date'])
full_prop_good.drop(columns=['FINCA','SECTOR','MUESTRA','CE'], inplace=True) 
seguimiento_lag_inds = seguimiento_lag_inds.merge(interpolated_propiedades, how='left', left_on=['name_c','date'], right_on=['name_c','date'])

seguimiento_lag_inds.loc[:,['TIPO DE FORRAJE']] = seguimiento_lag_inds.loc[:,['name_c','TIPO DE FORRAJE']].groupby(['name_c']).apply(lambda group: group.fillna(method='bfill'))
seguimiento_lag_inds.to_csv (destination+'/all_joined.csv', index = False, header=True)
'''