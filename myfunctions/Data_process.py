# -*- coding: utf-8 -*-
"""
Created on Sat Apr  3 16:46:36 2021

@author: Marcelo
"""
import datetime
import pandas as pd
import numpy as np
import json
from myfunctions.tools import Tools

class Data_Process:
    def Base_data(data, bands, Date_Ini, destination):
        
        #dates
        dates_list = Tools.lista_dates(data,'date',5) 
        #join dates and lotes-bands 
        base2 = Tools.self_join(dates_list, data,['lote_id','name_c','band'])
        full_data = base2.merge(data, how='left', left_on=['lote_id','name_c','band','date'], right_on=['lote_id','name_c','band','date'])
        
        #filter dates to create good dataset
        reliable_date=Date_Ini
        if reliable_date != 'no':
            full_data = full_data[full_data.date>=datetime.datetime.strptime(reliable_date,'%Y%m%d')]
        full_data = full_data.sort_values(by=['lote_id','band']) #order again
        data_ind = full_data[full_data['band'].isin(bands)] #filter for this bands
        data_ind.reset_index(drop = True, inplace=True)
        full_data.to_csv (destination+'/bandas_semanal.csv', index = False, header=True)
        full_data['Y-M'] = full_data['date'].dt.strftime('%Y-%m')
        full_data['date'] = full_data['date'].dt.strftime('%Y-%m-%d')
        #dic = {k: f.groupby('band')['mean_value'].apply(list).to_dict() for k, f in full_data.groupby('lote_id')} #full_data.to_dict('list')
        
        #Data mean to JSON file
        '''
        falta hacer mas nested por cliente, finca
        '''
        dic = {l: {k: f.groupby('band')['mean_value'].apply(list).to_dict() for k, f in v.groupby('Y-M')} for l, v in full_data.groupby('lote_id')} 
        dic2 = {l: {k: f.groupby('Y-M')['date'].apply(lambda x: sorted(set(x))).to_dict() for k, f in v.groupby('Y-M')} for l, v in full_data.groupby('lote_id')} #
        dic3 = Tools.merge_dicts(dict(dic), dic2)
        with open(destination+'/data.json', 'w') as outfile:
            json.dump(dic3, outfile,sort_keys=True, indent=4)
            
        return data_ind
    def seguimiento_lotes_proc(seguimiento, data, destination):
        
        seguimiento['FECHA ACTIVIDAD'] = pd.to_datetime(seguimiento['FECHA ACTIVIDAD'], format='%m/%d/%Y') #format the date
        seguimiento['FECHA ENTRADA'] = pd.to_datetime(seguimiento['FECHA ENTRADA'], format='%m/%d/%Y') #format the date
        seguimiento['FECHA SALIDA'] = pd.to_datetime(seguimiento['FECHA SALIDA'], format='%m/%d/%Y') #format the date
        seguimiento['FECHA PROYECTADA FUMIGACION'] = pd.to_datetime(seguimiento['FECHA PROYECTADA FUMIGACION'], format='%m/%d/%Y') #format the date
        
        #fix names to match data
        seguimiento['lote_c'] =  None
        #funcion para corregir nombre de finca
        seguimiento['lote_c'] =  seguimiento.apply(lambda row: Tools.finca_cor(row['FINCA'],row['POTRERO']), axis=1)
        seguimiento['lote_c'] = seguimiento['lote_c'].apply(lambda x: Tools.corr_num(x) if x is not None else x)
                 
        #filter dataframe for dates and keep usable data
        seguimiento.drop(columns=['AÑO','MES','FINCA','POTRERO','AREA M2','ANO FUMIGACION','MES FUMIGACION'], inplace=True)
        #reorder dataframe
        seguimiento = seguimiento.sort_values(by=['lote_c','FECHA ACTIVIDAD']) #order again
        cols = seguimiento.columns.tolist()
        cols = cols[-1:] + cols[1:2] + cols[:1] + cols[2:-1]
        seguimiento = seguimiento[cols]
        
        #Check if dates are different
        #seguimiento['en-sa'] = seguimiento['FECHA SALIDA'] - seguimiento['FECHA ENTRADA']
        #seguimiento['ac-en'] = seguimiento['FECHA ACTIVIDAD'] - seguimiento['FECHA ENTRADA']
        #seguimiento['di-to'] = seguimiento['LECHE TOTAL'] - seguimiento['LECHE POR DIA']
        seguimiento['LECHE VACA'] = seguimiento['LECHE TOTAL'] / seguimiento['NUMERO ANIMALES']
        #seguimiento['di-LE/VA'] = seguimiento['LECHE POR VACA'] - seguimiento['LECHE VACA']
        #equal dates, remove useless columns
        seguimiento.drop(columns=['FECHA SALIDA','FECHA ENTRADA','FORRAJE FRECIDO','FECHA PROYECTADA FUMIGACION','DIAS ESTADIA','LECHE POR DIA','LECHE POR VACA'], inplace=True) 
        
        #propiedades de pasto
        pasto = seguimiento.loc[seguimiento['ACTIVIDAD']=='PASTOREO',['lote_c','FECHA ACTIVIDAD','HATO','TIPO DE FORRAJE']]
        pasto.reset_index(drop = True, inplace=True)
        #join con lotes_id
        pasto = pasto.merge(lotes, how='left', left_on=['lote_c'], right_on=['nombre_lote'])
        pasto = pasto.loc[:,['lote_id','lote_c','FECHA ACTIVIDAD','TIPO DE FORRAJE']].groupby(['lote_id','lote_c','TIPO DE FORRAJE']).last()
        pasto.reset_index(inplace=True)
        pasto_actual = pasto.groupby(['lote_id','lote_c']).last()
        pasto_actual.reset_index(inplace=True); pasto_actual.drop(columns=['FECHA ACTIVIDAD'], inplace=True)
        pasto_actual.to_csv (destination+'/pasto_actual.csv', index = False, header=True)
        pasto.to_csv (destination+'/pasto_cambios.csv', index = False, header=True)
        '''
        pasto_actual subir a propiedades de tabla SQL
        '''
        
        #otras actividades para subir a SQL original
        otras = seguimiento.loc[seguimiento['ACTIVIDAD']!='PASTOREO',['lote_c','FECHA ACTIVIDAD','ACTIVIDAD','PRODUCTO']]
        otras.reset_index(inplace=True, drop=True)
        otras = otras.merge(lotes, how='left', left_on=['lote_c'], right_on=['nombre_lote'])
        '''
        Descargar tabla de actividades, con IDs y ajustar tabla 'otras'
        decargar tabla de productos y ajustar, renombrando productos y dividiendo producto / dosis / otro
        subir tabla otras a SQL como inicial
        '''
        
        #list dates at daily interval 
        dates_list = Tools.lista_dates(seguimiento,'FECHA ACTIVIDAD',1) 
        
        #join dates and lotes-bands        
        base2 = Tools.self_join(dates_list, data,['lote_id','nombre_lote'])
        base2.rename(columns={"nombre_lote": "name_c"}, inplace=True)
        #join base and real data SEGUIMIENTO
        full_seguimiento = base2.merge(seguimiento, how='left', left_on=['name_c','date'], right_on=['lote_c','FECHA ACTIVIDAD'])
        
        full_seguimiento = full_seguimiento.sort_values(by=['lote_id','date'])
        full_seguimiento.reset_index(drop = True, inplace=True)
        
        return full_seguimiento
    
    def lag_hatos(data, destination): #full_seguimiento
        data_hato = data.copy()
        #lag numero animales, lag dias de pastorep
        hato_lag = data_hato.loc[data_hato['ACTIVIDAD']=='PASTOREO',['name_c','actividad_change','ultimo_animales','dias_de_pastoreo']] #remove unused variables
        hato_lag2 = hato_lag.groupby(["name_c"]).shift(1)
        hato_lag2 = hato_lag2.loc[hato_lag['actividad_change']==False,:]
        hato_lag2.rename(columns={'ultimo_animales':'lag_ultimo_animales', 'dias_de_pastoreo':'lag_dias_de_pastoreo'}, inplace=True)
        
        data_hato = data_hato.join(hato_lag2.loc[:,['lag_ultimo_animales','lag_dias_de_pastoreo']],lsuffix='_l',rsuffix='_r')
        data_hato.loc[:,['lag_ultimo_animales','lag_dias_de_pastoreo']] = data_hato.loc[:,['name_c','ACTIVIDAD','lag_ultimo_animales','lag_dias_de_pastoreo']].groupby(['name_c','ACTIVIDAD']).apply(lambda group: group.fillna(method='pad'))
        
        data_hato = data_hato[data_hato['ACTIVIDAD']=='PASTOREO']
        data_hato.drop(columns=['name_c','FECHA ACTIVIDAD','ACTIVIDAD','PRODUCTO','DIAS ANTIGUEDAD PASTOS','lote_change','actividad_change'], inplace=True)
        data_hato = data_hato.sort_values(by=['HATO','date'])
        data_hato.reset_index(drop = True, inplace=True)
        
        #lags de nombres de lotes
        lag_lote = data_hato.loc[:,["lote_c",'HATO']]
        lag_lote = lag_lote.loc[lag_lote['lote_c'].shift()!=lag_lote['lote_c']]
        lag_lote['lag_1']= lag_lote.loc[:,["lote_c",'HATO']].groupby('HATO').shift()
        lag_lote['lag_2']= lag_lote.loc[:,["lote_c",'HATO']].groupby('HATO').shift(2)
        lag_lote['lag_3']= lag_lote.loc[:,["lote_c",'HATO']].groupby('HATO').shift(3)
        
        data_hato = data_hato.join(lag_lote.loc[:,['lag_1','lag_2','lag_3']],lsuffix='_l',rsuffix='_r')
        data_hato.loc[:,['lag_1','lag_2','lag_3']] = data_hato.loc[:,['HATO','lag_1','lag_2','lag_3']].groupby(['HATO']).apply(lambda group: group.fillna(method='pad'))
        #ultimo pastoreo y utlimo animales sobran 
        data_hato.drop(columns=['ultimo_pastoreo','ultimo_animales'], inplace=True) 
        
        solo_lotes = data_hato.loc[:,['date','HATO','lote_c','lag_1','lag_2','lag_3']]
        solo_lotes = pd.melt(solo_lotes, id_vars=['date','HATO'], value_vars=['lote_c', 'lag_1','lag_2','lag_3'])
        solo_lotes = solo_lotes.sort_values(by=['HATO','date'])

        #return hato_c
        solo_lotes['HATO_C'] =  solo_lotes.apply(lambda row: Tools.hato_cor(row['HATO'],row['variable']), axis=1)
        
        
        #Split dataframe solo lotes (drop leche, hato)
        data.drop(columns=['lote_c','FECHA ACTIVIDAD','DIAS ANTIGUEDAD PASTOS','AFORO','NUMERO ANIMALES','HATO','LECHE TOTAL','LECHE VACA','lote_change','actividad_change'], inplace=True) #'ACTIVIDAD'
        #si actividad es pastoreo, dias sin pastoreo = 0
        data['dias_sin_pastoreo']= np.where(data['ACTIVIDAD']=='PASTOREO', 0,data['dias_sin_pastoreo'])
        

        data.to_csv (destination+'/full_seguimiento.csv', index = False, header=True)
        data_hato.to_csv (destination+'/seguimiento_hatos.csv', index = False, header=True)
        solo_lotes.to_csv (destination+'/lag_lotes.csv', index = False, header=True)
        print('exported files csv')
        #EXPORTAR ACTIVIDAD-PRODUCTO
        '''
        tst = seguimiento.loc[:,['ACTIVIDAD','PRODUCTO']]
        res_actividades = tst.ACTIVIDAD.value_counts()
        res_productos = tst.PRODUCTO.value_counts()
        tst.drop_duplicates(inplace=True) 
        tst = tst.sort_values(by=['ACTIVIDAD','PRODUCTO'])
        tst.reset_index(drop = True, inplace=True)
        tst.to_csv (data_folder+'/ACTIVIDAD-PRODUCTO.csv', index = False, header=True)
        res_actividades.to_csv (data_folder+'/ACTIVIDAD-cantidades.csv', index = True, header=True)
        res_productos.to_csv (data_folder+'/PRODUCTO-cantidades.csv', index = True, header=True)
        '''
        return data
        
        