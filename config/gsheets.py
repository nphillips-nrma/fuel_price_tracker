

## google upload stuff
import gspread,json,os
from oauth2client.service_account import ServiceAccountCredentials


## PULL api keys
dir_keys = os.environ.get('AUTH_DIR') 
api_name= 'fuel-automation-v1-e62ad0c04d0d.json'


scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('{a}/{b}'.format(a=dir_keys,b=api_name), scope)
gsheet_client = gspread.authorize(creds)


