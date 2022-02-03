# -*- coding: utf-8 -*-
"""
Created on Fri Jul  9 10:09:23 2021

@author: Marcelo
"""
from twilio.rest import Client
from twilio.base.exceptions import TwilioRestException
import datetime


# Your Account SID from twilio.com/console
account_sid = "ACa8304315906d804a358f9bff26fc0a05"

# Your Auth Token from twilio.com/console
auth_token  = "98f0caedadd311d358924367a4e198c1"

#Twilio number
phoneSID = "PN94b59cb75dec21eb152966ce2023ab57"
twilioNumber = "+18018212669"

#Andres
'''
account_sid = "ACfa4fc4e8890a5283f889511b6a052745"
auth_token = "891606df94445ec3ea67aeccffcf987c"
twilioNumber = "+18178131257"
'''

hoy = datetime.date.today().strftime("%Y/%m/%d")
client = Client(account_sid, auth_token)
ID_VACA = 5082
texto = "Alerta! Deteccion de Celo en vaca: " + str(ID_VACA) + ' - hoy: ' + hoy
texto = "665544"
'''
try:
    message = client.messages.create(
        to="+97455033811", 
        from_= twilioNumber,
        body = texto #"Hello from Python!"
        )
    
    print(message.sid)

except TwilioRestException as e:
    # Implement your fallback code
    print(e)
    
#test
NUMBERS = {
        'Marcelo':'+97455033811',
        'Paol':'+97470378806',
        'JuanC':'+573112283946',
        'JuanG':'+573114812582'
        }

'''
   
#Se debe verificar cada numero, para usar en el Trial
NUMBERS = {
        'Marcelo':'+974 55033811',
        'Felipe':'+57 3108672598',
        'Andres':'+57 3173941374',
        'Nico':'+57 3102961475',
        'Fernando':'+57 3103128204'
        }
'''
NUMBERS = {
        'Marcelo_USA':'+18018212669'
        }

NUMBERS = {
    'Andres_tw': '+18178131257'
    
    }
'''

### With For Loop
for name, number in NUMBERS.items():
    try:
        message = client.messages.create(
            to=number, 
            from_=twilioNumber, 
            body=texto)
        print('exito')
        print (message.sid)
    except TwilioRestException as e:
        print(e)