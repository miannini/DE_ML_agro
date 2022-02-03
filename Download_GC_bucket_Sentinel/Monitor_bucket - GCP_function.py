# -*- coding: utf-8 -*-
"""
Created on Mon Jul 12 17:09:38 2021

@author: Marcelo
"""
#from google.oauth2 import service_account
from google.cloud import storage
import re
import pandas as pd
#import io
import os
#import json
#from pathlib import Path
import sqlalchemy as sa
from datetime import datetime, date, timedelta


######################################       DATABASE SQL    ##############################################
'''
DB_USER = 
PASSWORD =
IP =
#engine = sa.create_engine("mysql+pymysql://" + DB_USER+ ":" + PASSWORD + "@" + IP + "/" + "lv_test")
'''
engine = sa.create_engine("mysql+pymysql://" + os.environ['DB_USER']+ ":" + os.environ['PASSWORD'] + "@" + os.environ['IP']  + "/" + "lv_test")

#List of unique zones
sql = "select sentinel_zone from lv_test.Finca"
zonas = pd.read_sql(sql, engine)
zonas = zonas[zonas['sentinel_zone'].notnull()]
zonas = zonas[zonas['sentinel_zone'].str.contains("/")]
zonas = list(zonas.sentinel_zone.unique())

#list of downloaded dates zones and dates
sql_des = "select zona, fecha from lv_test.monitoreo_descargas_sentinel"
df_zonas = pd.read_sql(sql_des, engine)
df_zonas['zona'] = df_zonas['zona'].str.split('/',expand=False).str.join('')
df_zonas['key'] = df_zonas['zona']+ df_zonas['fecha'].astype(str)
df_zonas['key2'] = "T" + df_zonas['zona'] + "_" + df_zonas['fecha'].astype(str).str.split('-',expand=False).str.join('')
print("total historic data count: ",len(df_zonas['key']))

#################################    Auth with GCP   ################################################
'''
with open('../secrets/data-science-proj-280908-e7130591b0d5.json') as source:
    info = json.load(source)
    project_id = 'data-science-proj-280908'
storage_credentials = service_account.Credentials.from_service_account_info(info)
storage_client = storage.Client(project=project_id, credentials=storage_credentials)
'''
storage_client = storage.Client()

#################################    functionss       #################################################

### List all blobs of main bucket folder containing year / month filter
def list_all_blobs(storage_client, bucket_name, prefix=None, delimiter=None, year=None, month=None, sensor=None): #prefix is initial route, delimiter is '/', year, month
    lista = []
    #storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix,delimiter=delimiter)
    for blob in blobs:
        if year is not None: #si ha restriccion de year
            if month is not None: #tambien hay restriccion de month
                if re.search(r'_'+re.escape(year)+re.escape(month),blob.name):
                    tmp_name = blob.name.split('$')[0][:-1] #removes _$.folder$
                    lista.append(tmp_name)
            else:               #no hay restriccion de mes
                if re.search(r'_'+re.escape(year),blob.name):
                    tmp_name = blob.name.split('$')[0][:-1]
                    lista.append(tmp_name)
        else:                   #no hay restriccion de year
            if month is not None:    #si hay restriccion de mes
                if re.search(r'_****'+re.escape(month),blob.name):
                    tmp_name = blob.name.split('$')[0][:-1]
                    lista.append(tmp_name)
            else:               #no hay restriccion de year ni month
                tmp_name = blob.name.split('$')[0][:-1]
                lista.append(tmp_name)  
    return lista


### Copy blobs by name, defining destination name
def copy_blobs_sate(storage_client, bucket_name, source_blob_name, destination_buck, destination_file_name):
    """Downloads a blob from the bucket."""
    #storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    destination_bucket = storage_client.bucket(destination_buck)
    try: #basic copy blob function
        new_blob = bucket.copy_blob(blob,destination_bucket, new_name=destination_file_name)#, timeout=300.0)
    except:  #in case of large files >100MB
        dest_blob = destination_bucket.blob(destination_file_name) #create destination file, based on dest bucket and new name
        rewrite_token = False
        while True:
            rewrite_token, bytes_rewritten, bytes_to_rewrite = dest_blob.rewrite(blob, token=rewrite_token) #rewrite function to copy large files
            print(f"\t{destination_file_name}: Progress so far: {bytes_rewritten}/{bytes_to_rewrite} bytes.")
            if not rewrite_token: #when finishes to copy, breaks
                break  
    
########################################    CALLS     #####################################################

images_route = 'L2/tiles/' #route base = L2/tiles/ + zone 18NXM for this customer
#zone = '18/N/XM/'
#zone2=''.join(zone.split('/'))
#id_cliente = 1 #not required at this moment
buck= 'gcp-public-data-sentinel-2'  #bucket for satellite sentinel data
year  = str(date.today().year)
month = str(date.today().month).zfill(2)

year2 = str((date.today()- timedelta(days=20)).year)
month2 = str((date.today()- timedelta(days=20)).month).zfill(2)
#year='2021'   #custom filter user-defined or by cloud function
#month='05'    #custom filter user-defined or by cloud function

### call function based on previous filters
datos=[]
for zone in zonas:
    for (mes, ano) in ((month,year),(month2,year2)):
        datos_tmp = list_all_blobs(storage_client,buck,images_route+zone,'/', year=ano, month=mes) #,'/'
        #datos.append(datos_tmp)
        datos = datos + datos_tmp

#remove datos in df_zonas
matching = [s for s in datos if not any(xs in s for xs in df_zonas['key2'])]


### bands required based on resolution size
at10 = ['AOT','B02','B03', 'B04', 'B08', 'TCI', 'WVP']      #bands at 10m resolution
at20 = ['B05', 'B06', 'B07', 'B11', 'B12', 'B8A', 'SCL']    #bands at 20m resolution
at60 = ['B01', 'B09', 'B10']                                #bands at 60m resolution
Res = ['R10m','R20m', 'R60m']                               #resoluction ranges (folder names)
sizes = [at10, at20, at60]                                  #list of lists of bands
resol = pd.DataFrame(zip(Res,sizes))                        #dataframe to loop through 


### loop to find folder names
files_bnd=[]    #list of folder names in GRANULE folder
for dato in matching:  #based on previous found folders with filtered data
    for size in sizes:
        fold = storage_client.list_blobs(buck, prefix=dato+'/GRANULE/',delimiter='/')
        #print(fold)
        for blob in fold:
            tmp_name2 = blob.name.split('$')[0][:-1] #removes _$.folder$
            files_bnd.append(tmp_name2)
        
files_bnd = list(set(files_bnd))    #set to avoid duplicates


### loop to find useful jp2 files
files_fin=[]
for flie in files_bnd:  #based on previous found folders
    for R in range(0,len(resol)):   #resolution dataframe
        #new folder found based on resolution
        fold2 = storage_client.list_blobs(buck, prefix=flie+'/IMG_DATA/'+resol[0][R]+'/',delimiter='/')
        for blob in fold2:
            #list of blobs in folder, and filter based on required bands on each resolution
            for banda in resol[1][R]:
                if re.search(r'_'+re.escape(banda)+r'_',blob.name):
                    files_fin.append(blob.name)

print("total files count: ",len(files_fin))
    

### copy each file, in the corresponding folder, and edit name to avoid resolution comment
my_own_buck= 'satellite_storage'
dates_df=pd.DataFrame(columns=['zona','file','fecha','process_date'])
for n in files_fin: #range(36,70):
    #Zone
    zone2 = "".join(n.split('/')[2:5])
    #new name               zone code        dated_folder,                                           rejoin after =    split name, remove resolution, add .jp2
    new_name = 'Raw_images/' + zone2 + '/' + n.split('/')[-1].split('_')[1][:8] +"/"+os.path.basename('_'.join(n.split('/')[-1].split('_')[:-1])+'.jp2')
    #Date of file
    tmp_date = n.split('/')[-1].split('_')[1][0:8]
    date2 = datetime.strptime(tmp_date, '%Y%m%d').date().strftime("%Y-%m-%d")
    #compare zone2+date2 vs df_zonas['key] to check if area+date have been downloaded previously
    if not df_zonas['key'].str.contains(zone2+date2).any():
        print(date2,'ok')
        #copy function               
        copy_blobs_sate(storage_client,buck, n, my_own_buck, new_name)
        #create a list of unique processed dates
        if not dates_df['fecha'].str.contains(date2).any(): 
            file= '_'.join(n.split('/')[-1].split('_')[0:2])
            dates_df_tmp = pd.DataFrame([["/".join(n.split('/')[2:5])+'/', file, date2, datetime.now().strftime("%Y-%m-%d %H:%M:%S")]], columns=['zona','file','fecha','process_date'])
            dates_df_tmp.to_sql('monitoreo_descargas_sentinel', engine,  if_exists='append', index=False)   
            dates_df = pd.concat([dates_df,dates_df_tmp], ignore_index=True)
    else:
        print(date2,'Already in DB')
        
