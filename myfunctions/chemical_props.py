# -*- coding: utf-8 -*-
"""
Created on Sun Apr  4 21:59:29 2021

@author: Marcelo
"""

import datetime
import pandas as pd
import numpy as np
from myfunctions.tools import Tools

class Chemical_props:
    def Chem_analysis(propiedades,date_ini):
        
        #ajustar fecha
        propiedades['FECHA'] = pd.to_datetime(propiedades['FECHA'], format='%m/%d/%Y') #format the date
        #dividir por lote
        new = propiedades["SECTOR"].str.split(":", n = 1, expand = True) 
        new2 = new[0].str.split(" ", n = 1, expand = True) 
        lotes_joined = pd.DataFrame(propiedades["FINCA"]).join(new, lsuffix='_ori',rsuffix='_new')
        lotes_joined = lotes_joined.join(new2, lsuffix='_ori',rsuffix='_new')
        
        #si 1_ori tiene numeros separados por "," usarlo, sino usar 1_new
        numbers = lotes_joined["1_ori"].str.split(",", expand = True) 
        numbers2 = lotes_joined["1_new"].str.split(",", expand = True) 
        numbers = numbers.join(numbers2,lsuffix='_l',rsuffix='_r')
        numbers = numbers.apply(pd.to_numeric,errors='ignore')
        
        
        df_numbers=[]
        for n in range(0,numbers2.shape[1]):
            df_num=[]
            for index, row in numbers.iterrows():
                df_num.append(Tools.chem_correct_num(row['{}_l'.format(n)],row['{}_r'.format(n)]))
            if n==0:
                df_numbers = pd.DataFrame(df_num)
            else:
                df_numbers = pd.concat([df_numbers,pd.DataFrame(df_num)], axis=1)
        
        #join with more columns that were not compared
        df_numbers = pd.concat([df_numbers,numbers.iloc[:,range(numbers2.shape[1],numbers.shape[1]-numbers2.shape[1])]], axis=1)
        #rest column names
        df_numbers.columns = range(df_numbers.shape[1])
        
        #fix names to match data
        lotes_joined['FINCA_C'] =  None
        for n in range(0,len(lotes_joined)):
            if lotes_joined.loc[n,'FINCA'] == 'RECODO':
                lotes_joined.loc[n,'FINCA_C'] = 'RC'
            elif lotes_joined.loc[n,'FINCA'] == 'ISLA':
                if 'JUNCAL' in lotes_joined.loc[n,'0_ori'] :
                    lotes_joined.loc[n,'FINCA_C'] = 'Juncalito-Lote_'
                elif 'PARAISO' in lotes_joined.loc[n,'0_ori']:
                    lotes_joined.loc[n,'FINCA_C'] = 'Paraiso-Lote_'
                elif 'MANGA LARGA' in lotes_joined.loc[n,'0_ori']:
                    lotes_joined.loc[n,'FINCA_C'] = 'La_Isla-Lote_ML_'
                #elif 'LA ISLA' in lotes_joined.loc[n,'0_ori']:
                #    lotes_joined.loc[n,'FINCA_C'] = 'La_Isla-Lote_' 
                #elif 'RANCHO' in lotes_joined.loc[n,'0_ori']:
                #    lotes_joined.loc[n,'FINCA_C'] = 'La_Isla-Lote_' 
                else:
                    lotes_joined.loc[n,'FINCA_C'] = 'La_Isla-Lote_' 
        
        #join finca_c and lote number, row by row, then expand in vertical form by name
        lotes_joined = pd.DataFrame(lotes_joined["FINCA_C"]).join(df_numbers, lsuffix='_a',rsuffix='_b')
        
        '''
        def union_nombres(finca, number):
            if finca == 'RECODO' and number <10:
                name = finca+'0'+str(number)
            else:
                name = finca+str(number)
            return name
        '''
        df_names=[]
        
        #for index, row in lotes_joined.iterrows():
        #        df_names.append(union_nombres(row['FINCA_C'],row[]))
        #lotes_joined2 = lotes_joined.iloc[:,1:].apply(lambda x: x.apply(lambda y: "{}{}".format(lotes_joined.iloc[x,0], y)))
        
        for n in range(0,len(lotes_joined)):
            for m in range(1,lotes_joined.shape[1]):
                if pd.isna(lotes_joined.iloc[n,m]) == False:
                    #if (lotes_joined.iloc[n,m] < 10 and lotes_joined.iloc[n,0] == 'RC'):
                    if (lotes_joined.iloc[n,m] < 10 and "ML_" not in lotes_joined.iloc[n,0]):
                        lotes_joined.iloc[n,m] = lotes_joined.iloc[n,0] + '0' + str(int(lotes_joined.iloc[n,m]))
                    else:
                        lotes_joined.iloc[n,m] = lotes_joined.iloc[n,0] + str(int(lotes_joined.iloc[n,m]))
        lotes_joined = lotes_joined.iloc[:,1:]
        
        # join index and lotes
        lotes = lotes_joined.stack()
        lotes = lotes.reset_index(level=[0,1])
        lotes = lotes.set_index('level_0')
        lotes.drop(columns=['level_1'], inplace=True) 
        
        full_prop = lotes.join(propiedades,lsuffix='_l',rsuffix='_r')
        #rename columns
        full_prop.rename(columns={0:'name_c', 'FECHA':'date'}, inplace=True)
        full_prop = full_prop.sort_values(by=['name_c','date'])
        
        #min date for interpolation
        #list dates at daily interval
        '''
        min_date3 = datetime.datetime.strptime(date_ini, '%Y-%m-%d')#(2019, 01, 01)
        len_dates3 = int((full_prop['date'].max()-min_date3).days / 1)
        dates_list3 = []
        for n in range(0,len_dates3+1):
            if n == 0:
                fecha = min_date3
            else:
                fecha = fecha + datetime.timedelta(days=1)
            dates_list3.append(fecha)
        '''
        dates_list = Tools.lista_dates(full_prop,'date',1) 
        dates_list = [x for x in dates_list if x>=datetime.datetime.strptime(date_ini, '%Y%m%d')] #'2012-10-01'
        #join dates and lotes-bands
        base2 = Tools.self_join(dates_list, full_prop, ['name_c'])
        
        '''
        base5 = full_prop.loc[:,['name_c']]
        base5.drop_duplicates(inplace=True)    
        base6=pd.DataFrame()    
        for m in dates_list3:
            base_t = base5.copy()
            base_t['date'] = m
            base6 = pd.concat([base6,base_t],ignore_index=True, axis=0)
        '''
        #join base and real data SEGUIMIENTO
        full_prop_old = base2.merge(full_prop, how='left', left_on=['name_c','date'], right_on=['name_c','date'])
        full_prop_old = full_prop_old.sort_values(by=['name_c','date'])
        full_prop_old.reset_index(drop = True, inplace=True)
        #interpolate values based on previous row and same name
        full_prop_old = full_prop_old.groupby('name_c').apply(lambda group: group.fillna(method='pad'))
        
        #list of dates with change in properties
        dates_change = propiedades.loc[:,'FECHA'].drop_duplicates(inplace=False)
        dates_change = dates_change[dates_change >= datetime.datetime.strptime(date_ini,'%Y%m%d')]
        dates_change.reset_index(drop = True, inplace=True)
        
        list_elements = full_prop_old.columns.to_list()[7:]
        
        #filter dates > 2019-01-01
        #reliable_date='20121001'
        #full_prop_good = full_prop_old[full_prop_old.date>=datetime.datetime.strptime(date_ini,'%Y%m%d')]
        total_cells = np.product(full_prop_old.shape)
        na_cells = full_prop_old.isnull().sum().sum()
        na_perc = na_cells/total_cells
        print("[INFO] total data = {:.2f}, missing data = {:.2f} - (% miss data= {:.2f})".format(total_cells,na_cells,na_perc))
        full_prop_old.dropna(thresh=10,inplace=True)
        
        return full_prop_old, dates_change, list_elements, dates_list, full_prop