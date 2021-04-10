# -*- coding: utf-8 -*-
"""
Created on Wed Mar 31 21:25:40 2021

@author: Marcelo
"""

import requests
import pandas as pd
from . import secrets

url = secrets.URL

class API_usage:
    def login_api():
        hed = {'Content-Type': 'application/x-www-form-urlencoded'} #'accept': 'application/json',
        data = {'username':secrets.USERNAME,
                'password':secrets.PASS}
        
        #data = {'username':'miannini',
        #        'password':'admin1234'}
        #data_ready = '&'.join('{}:{}'.format(key, value) for key, value in data.items())
        
        url = secrets.URL
        #url = 'https://data-science-proj-280908.ue.r.appspot.com/'
        login = 'token'
        
        response = requests.post(url+login, data=data, headers=hed)
        if response.status_code == 200:
            print('access succesful')
            token = response.json()['access_token']
            bearer = response.json()['token_type']
        else:
            print('error')
        
        return token
    
    def get_clientes(token):
        hed = {'accept': 'application/json',
               'Authorization': "Bearer "+token}
        path = 'clientes/'
        response = requests.get(url+path, headers=hed)
        print(response.status_code)
        clientes = []
        if response.status_code == 200:
            data_list = response.json()#['access_token']
            for dic in data_list:
                for key in dic:
                    if key == 'ID_CLIENTE':
                        clientes.append(dic[key])
        else:
            print('error')
        return clientes
    
    def get_lotes(token, coords=False):
        hed = {'accept': 'application/json',
               'Authorization': "Bearer "+token}
        path = 'Lotes/'
        response = requests.get(url+path, headers=hed)
        print(response.status_code)
        lotes = []; fincas = []; nombres = []; latitud=[]; longitud=[]
        if response.status_code == 200:
            data_list = response.json()#['access_token']
            for dic in data_list:
                for key in dic:
                    if key == 'ID_LOTE':
                        lotes.append(dic[key])
                    elif key == 'ID_FINCA':
                        fincas.append(dic[key])
                    elif key == 'NOMBRE_LOTE':
                        nombres.append(dic[key])
                    elif coords==True and key == 'LATITUD':
                        latitud.append(dic[key])
                    elif coords==True and key == 'LONGITUD':
                        longitud.append(dic[key])
                        
            if coords==False:
                df = pd.DataFrame(list(zip(fincas, lotes, nombres)),
                              columns =['Finca', 'lote_id', 'nombre_lote']) 
            else:
                df = pd.DataFrame(list(zip(fincas, lotes, nombres, latitud, longitud)),
                              columns =['Finca', 'lote_id', 'nombre_lote', 'x', 'y'])
        else:
            print('error')
        return df
    
    def get_hatos(token):
        hed = {'accept': 'application/json',
               'Authorization': "Bearer "+token}
        path = 'Hatos/'
        response = requests.get(url+path, headers=hed)
        print(response.status_code)
        hatos = []; fincas = []; nombres = []; tipos = []
        if response.status_code == 200:
            data_list = response.json()#['access_token']
            for dic in data_list:
                for key in dic:
                    if key == 'ID_HATO':
                        hatos.append(dic[key])
                    elif key == 'ID_FINCA':
                        fincas.append(dic[key])
                    elif key == 'Nombre_Hato':
                        nombres.append(dic[key])
                    elif key == 'TIPO_Hato':
                        tipos.append(dic[key])    
            df = pd.DataFrame(list(zip(fincas, hatos, nombres, tipos)),
                              columns =['Finca', 'hato_id', 'nombre_hato', 'tipo_hato'])              
        else:
            print('error')
        return df
        
        
        
        
         