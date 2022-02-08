# -*- coding: utf-8 -*-
"""
Created on Fri Jul  9 10:09:23 2021

@author: Marcelo
"""
from twilio.rest import Client
from twilio.base.exceptions import TwilioRestException
import datetime
from ..secrets.secret_keys import account_sid, auth_token, phoneSID, twilioNumber


# Your Account SID from twilio.com/console
#account_sid = "SID"

# Your Auth Token from twilio.com/console
#auth_token  = "token"

#Twilio number
#phoneSID = "number from secrets"
#twilioNumber = "number UsA"


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