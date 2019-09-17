
# from urllib.request import Request,urlopen
import urllib.parse
import os,time,json
import requests 
import time 
import pandas as pd 
import numpy as np
import base64

############################################################
## FUNCTION to get all all QUERIES 
## https://api.nsw.gov.au/fuel-price-check/apiss

## PULL api keys
dir_keys = 'H:/admin/authentication' 
api_name= 'platts.json'

f= open('{a}/{b}'.format(a=dir_keys,b=api_name),"r")
apikey = json.loads(''.join([x for x in f ]))




s = requests.session()


platts_login = "https://pmc.platts.com/Login.aspx"
login_id = {'ctl00_cphMain_rfvEmail':apikey.get('USER'),'ctl00_cphMain_rfvPassword':apikey.get('PW')}

s_login = s.post(platts_login, login_id)

## 
s_login.status_code
s_login.cookies


dash_page = s.get('https://pmc.platts.com/MQT/MQTHome.aspx?nl=Market%20Data%20Snapshot&nl2=Snapshot&nl4=More%20Products')

dash_page.status_code
dash_page.text

https://pmc.platts.com/MQT/MQTHome.aspx?nl=Market%20Data%20Snapshot&nl2=Snapshot&nl4=More%20Products#
https://pmc.platts.com/MQT/MQTHome.aspx?nl=Market%20Data%20Snapshot&nl2=Snapshot&nl4=More%20Products#




headers = "appKey:{}".format(apikey.get('API_KEY'))


##
auth_url = 'https://api.onegov.nsw.gov.au/oauth/client_credential/accesstoken'
auth_payload = base64.b64encode('{u}:{p}'.format(u=apikey.get('consumer_key'),p=apikey.get('consumer_secret')).encode()).decode()
headers = {'authorization':auth_payload}
auth_return = requests.get(auth_url,headers=headers,params={'grant_type':'client_credentials'})
auth_return = auth_return.json()

## PREPARE api

auth_return.get('access_token')

fuel_url ='https://api.onegov.nsw.gov.au/FuelPriceCheck/v1/fuel/prices'
response = requests.get(project_url,headers=headers,
    params={'q':" SELECT * FROM road_traffic_counts_yearly_summary   "})

print(response.status_code)
payload = json.loads(response.text)
payload_df  = pd.DataFrame(payload.get('rows')) 


