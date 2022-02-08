# -*- coding: utf-8 -*-
"""
Created on Wed Aug 11 16:57:05 2021

@author: Marcelo
"""
from datetime import datetime
from datetime import timedelta
import os
import pandas as pd
from twilio.rest import Client
from ..secrets.secret_keys import account_sid, auth_token, phoneSID, twilioNumber


# Find your Account SID and Auth Token at twilio.com/console
# and set the environment variables. See http://twil.io/secure
#account_sid = os.environ['TWILIO_ACCOUNT_SID']
#auth_token = os.environ['TWILIO_AUTH_TOKEN']
client = Client(account_sid, auth_token)

#filtro de fechas
hoy = datetime.today()
hoy = hoy.replace(hour=0, minute=0, second=0, microsecond=0) # Returns a copy
ayer = hoy - timedelta(days=1)
manana = hoy + timedelta(days=1)

#messages = client.messages.list()
messages = client.messages.list(date_sent=ayer,limit=20)
messages2 = client.messages.list(date_sent=hoy,limit=20)
messages3 = client.messages.list(date_sent=manana,limit=20)
messages = messages + messages2 + messages3
col_names = ['body','direction','status','to','from','date_sent']
df_mesage = pd.DataFrame()
for record in messages:
    list_mesage = [record.body, record.direction, record.status, record.to, record.from_, record.date_sent]
    df_mesage = df_mesage.append([list_mesage])
    
df_mesage.reset_index(drop=True, inplace=True)
df_mesage.columns = col_names

recibidos = df_mesage[df_mesage['direction']=='inbound']
recibidos.reset_index(drop=True, inplace=True)
recibidos.sort_values('date_sent',ascending=False, inplace=True)
ultimo_reci = recibidos.iloc[0,:]
text_recib = ultimo_reci.body
''' parsear el body, para leer parametros y cargar en DB '''
