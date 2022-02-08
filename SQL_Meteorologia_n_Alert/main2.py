# -*- coding: utf-8 -*-
"""
Created on Wed Jun 23 21:55:49 2021

@author: Marcelo
"""
import pandas as pd
import numpy as np
import sqlalchemy as sa
from datetime import datetime
from datetime import timedelta
from ..secrets.secret_keys import EMAIL_API_KEY

DB_USER = 'm4a.MI'
PASSWORD = 'm4a2020'
IP = '35.185.0.199'
#EMAIL_API_KEY = 'from secrets'

'''
db_user = os.environ["DB_USER"]
db_pass = os.environ["DB_PASS"]
db_name = os.environ["DB_NAME"]
db_socket_dir = os.environ.get("DB_SOCKET_DIR", "/cloudsql")
cloud_sql_connection_name = os.environ["CLOUD_SQL_CONNECTION_NAME"]

pool = sqlalchemy.create_engine(
    # Equivalent URL:
    # mysql+pymysql://<db_user>:<db_pass>@/<db_name>?unix_socket=<socket_path>/<cloud_sql_instance_name>
    sqlalchemy.engine.url.URL.create(
        drivername="mysql+pymysql",
        username=db_user,  # e.g. "my-database-user"
        password=db_pass,  # e.g. "my-database-password"
        database=db_name,  # e.g. "my-database-name"
        query={
            "unix_socket": "{}/{}".format(
                db_socket_dir,  # e.g. "/cloudsql"
                cloud_sql_connection_name)  # i.e "<PROJECT-NAME>:<INSTANCE-REGION>:<INSTANCE-NAME>"
        }
    ),
    **db_config
)
'''
#min_date = '2021-05-01'
#max_date = '2021-06-10'
min_date = (datetime.today()-timedelta(days=30)).strftime("%Y-%m-%d")
max_date = datetime.today().strftime("%Y-%m-%d")
engine = sa.create_engine("mysql+pymysql://" + DB_USER+ ":" + PASSWORD + "@" + IP + "/" + "lv_test")
sql = "select * from lv_test.Meteorologia where FECHA_HORA >= '" + str(min_date) + "' and FECHA_HORA <= '" + str(max_date) +"'"
actDB = pd.read_sql(sql, engine)

datab = actDB.copy()
#datab['dif'] = (datetime.strptime(max_date,'%Y-%m-%d') - datab['FECHA_HORA']).dt.days
resu = datab.iloc[:,0:3].groupby(['ID_Estacion']).agg(Maximum_Date=('FECHA_HORA', np.max)) # ,'dif'
resu['mail']=0
resu.reset_index(inplace=True)

resu['dif'] = (datetime.strptime(max_date,'%Y-%m-%d') - resu['Maximum_Date']).dt.days
for n in range(len(resu)):
    print (n)
    resu['mail'][n] = 1 if resu['dif'][n] > 1 else 0
#alpha=1
def email(alpha, fecha):
    #import os
    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import Mail, Email
    from python_http_client.exceptions import HTTPError

    sg = SendGridAPIClient(EMAIL_API_KEY) #(os.environ['EMAIL_API_KEY'])

    html_content = "<p>ALERT - Meteo Station not transmiting data!</p>" + "<p>This email is to Alert for Meteo Station = " + str(alpha) + "</p>" + "<p>last received date = " + str(fecha) + "</p>"
                    

    message = Mail(
        to_emails=["jgarboleda@gmail.com", "juancacamacho89@gmail.com", "luchofelipe8023@gmail.com", "marceloiannini@hotmail.com", "nickair90@gmail.com"],
        from_email=Email('mianninig@gmail.com', "MyKau Monitor_Report"),
        subject="Meteo Station [AUTO ALERT] - MyKau",
        html_content=html_content
        )
    message.add_bcc("mianninig@gmail.com","minds4analytics@gmail.com","salsandres22@gmail.com")

    try:
        response = sg.send(message)
        return f"email.status_code={response.status_code}"
        #expected 202 Accepted

    except HTTPError as e:
        return e.message       

for n in range(len(resu)):  
    if resu['mail'][n]==1:
        if resu['ID_Estacion'][n]==1:
            alpha ="1 - Juan Camilo"
            email(alpha, resu['Maximum_Date'][n])
        elif resu['ID_Estacion'][n]==2:
            alpha ="2 - Juan Guillermo"
            email(alpha, resu['Maximum_Date'][n])