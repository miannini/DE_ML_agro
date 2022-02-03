# -*- coding: utf-8 -*-
"""
Created on Sat Apr 10 12:38:55 2021

@author: Marcelo
"""

'''
0- load packages, define initial variables
'''
#import os
#import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import datetime
import time
from pathlib import Path
from ml_funcs import ML_interpol
from myfunctions import API_usage
#from myfunctions import GCP_Functions
from myfunctions import Chemical_props
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
#Date_Ini= '20120101'
#Date_Fin= datetime.date.today().strftime("%Y%m%d")

'''
1- Function for login to API, store token and get Clientes, lotes
'''
token = API_usage.login_api()
clientes = API_usage.get_clientes(token)
lotes = API_usage.get_lotes(token, coords=True)

'''
1-b loop throught clientes and read lotes
'''
clientes = clientes[0:1]
#define user
for client in clientes:
    print (client)
    cliente = 'ID_CLIENTE-'+str(client)
    analysis_area = '../db_'+cliente+'/'


#for cloud files //  Storage para subir datos que se usen a futuro en modelos, o para Dash
bucket_files = 'satellite_storage'
folder ='Data/Database/'
pref = 'db_lotes_propiedades' #change this
dest= '../Data/Database_ready/'
#listar de Cloud storage
#objetos = list(GCP_Functions.list_all_blobs(bucket_files,prefix=folder+cliente+'/'+pref,delimiter='/')) #list fro cloud storage
destination = dest + cliente 
Path(destination).mkdir(parents=True, exist_ok=True) #create folder


'''
2- read propiedades del suelo
si se reciben nuevos datos, correr esta parte y modelos, (en archivo separado con pub/sub + Dataflow o cloud func)
esto debe quedar en SQL de Google cloud
la app debe permitir subir datos ingresados como texto [multiples lote a la vez posiblemente]
'''

date_chem_ini = Date_Ini #'2012-10-01'
ext_data_folder='../../20190601-20200630-simijaca/Database/'+'external_files'
propiedades = pd.read_csv(ext_data_folder+'/db_lotes_propiedades.csv')

full_prop_good, dates_change, list_elements, dates_list, full_prop = Chemical_props.Chem_analysis(propiedades,date_chem_ini) #'2012-10-01')

'''
full_prop, unir lote_id, quitar finca y poner finca_id, sector como comentarios
quitar anio y CE
hacer analisis de correlacion entre elementos
API lista: Wr_lotes_quimicos

version resumida de 'resumen_lotes_medidas' enviar a database usando API
    start=time.time()
    #define columns, rename as per API and format dates 
    reduced_columns=['lote_id','date','mean_value._bm','mean_value._cp','mean_value._ndf','mean_value._lai','mean_value._ndvi','cld_percentage','area_factor','biomass_corrected']
    reduced_flat = flattened.loc[:,flattened.columns.isin(reduced_columns)]
    reduced_flat.rename(columns={'lote_id':'ID_lote','date':'fecha','mean_value._bm':'Mean_BM','mean_value._cp':'Mean_CP','mean_value._ndf':'Mean_NDF','mean_value._lai':'Mean_LAI','mean_value._ndvi':'Mean_NDVI'}, inplace=True)
    reduced_flat['fecha'] = pd.to_datetime(reduced_flat['fecha'], format="%Y%m%d").dt.strftime('%Y-%m-%d')
    #reorder columns as per API
    cols = reduced_flat.columns.tolist()
    cols = cols[1:2] + cols[0:1] + cols[2:]
    reduced_flat = reduced_flat[cols] 
    #to dictionary
    data_dict = reduced_flat.to_dict(orient='records')
    #loop each row, remove nan and post API
    for data_row in data_dict:
        to_del=[]
        for k,v in data_row.items():
            if type(v) == float and math.isnan(v):
                print(k,'delete')
                to_del.append(k)
        for n in to_del:
            data_row.pop(n)
        API_usage.post_lote_var(token,data_row)
    end = time.time()
    print(end - start) 
'''

##########################################################################################################3
#with distance matrix, interpolate values of missing zones of different components
data_geo = lotes.loc[:,['nombre_lote','x','y']]
data_geo.rename(columns={'nombre_lote':'name_c'}, inplace=True)
#data_geo.drop_duplicates(subset='name_c', keep="last",inplace=True)


################################################################################################################3
#Machine learning model 
#call model // needs = list_elements, dates_change, full_prop_good, data_geo
'''
3- ML model
'''
dest= '../Data/ML_models/'
ml_destination = dest + cliente 
Path(ml_destination).mkdir(parents=True, exist_ok=True)
#dest_folder = ml_destination

'''
if no models, create the model
if model exists, load and predict
'''
start = time.time() 
df_fechas_elems, df_best_models = ML_interpol.space_interpol(full_prop_good,data_geo,dates_change, list_elements,ml_destination)
end = time.time()
print('elapsed time {:.2f}'.format(end-start))

df_fechas_elems = df_fechas_elems.merge(data_geo, how='left', left_on=['x','y'], right_on=['x','y'])
#export data
df_fechas_elems.to_csv (destination+'/propiedades_suelo_histo.csv', index = False, header=True)
df_best_models.to_csv (destination+'/mejores_modelos.csv', index = False, header=True, sep=';')
df_best_models.reset_index(drop = True, inplace=True)


'''
4- data visualization and charts
'''
#take dicts out of column for better analysis
df2 = pd.concat([df_best_models.drop('Model_parameters', axis=1), pd.json_normalize(df_best_models['Model_parameters'])], axis=1 )
#plot models parameters, to reduce grid_search
df2.epsilon.value_counts(normalize=True).plot.barh()
df2.gamma.value_counts(normalize=True).plot.barh()
df2.C.value_counts(normalize=True).plot.barh()
df2.kernel.value_counts(normalize=True).plot.barh()

#not working too good this hist
df2.hist(figsize=(14,14))#, xrot=45)
plt.show()

#sorted for scater plot
df2 = df2.sort_values(by=['Elemento','Fecha']); df2.reset_index(drop = True, inplace=True)

colors = {'Aluminio':'blue', 'Azufre':'red', 'Boro':'cyan', 'Calcio':'azure', 'Cloro':'grey', 'Cobre':'goldenrod', 'Fosforo':'coral',
          'Hierro':'black', 'Magnesio':'royalblue', 'Manganeso':'magenta', 'Nitrogeno':'green', 'PH':'teal', 'Potasio':'olive', 'Sodio':'indigo', 'Zinc':'yellow'}


plt.scatter(df2.index, df2['Model_accuracy'], #alpha=0.3,
            c= df2['Elemento'].apply(lambda x: colors[x])) 


#join data
#dates_list
'''
5- interpolar en fechas faltantes con PAD
esto queda muy grande y pesado, evaluar si se puede eiliminar
codigo abajo usar para cuando se necesite en el modelo de ML
'''
base2 = Tools.self_join(dates_list, data_geo,['name_c','x','y']) #['lote_id','name_c','band'])

interpolated_propiedades = base2.merge(df_fechas_elems, how='left', left_on=['x','y','name_c','date'], right_on=['x','y','name_c','date'])
interpolated_propiedades = interpolated_propiedades.sort_values(by=['name_c','date'])
#pad fill
interpolated_propiedades = interpolated_propiedades.groupby('name_c').apply(lambda group: group.fillna(method='pad'))
interpolated_propiedades = interpolated_propiedades[interpolated_propiedades.date>=datetime.datetime.strptime(Date_Ini,'%Y%m%d')]# min_date2]
interpolated_propiedades.reset_index(drop = True, inplace=True)
interpolated_propiedades.to_csv (destination+'/propiedades_suelo_todo.csv', index = False, header=True)

#del(df_fechas_elems,base2,data_geo,interpolated_propiedades)