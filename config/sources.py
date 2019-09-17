
import pandas as pd
import numpy as np
import re,time,os,sys,shutil
import time,datetime,math

config = {
   'home_dir': os.environ.get("FUEL_DIR")
,   'download_dir':"downloads"  # assuming already in '~/fuel_automation'
,   'staging_dir':"staging"  # assuming already in '~/fuel_automation'
,   'final_fuelset':['DIESEL','PULP95',"PULP98",'ULP',"E10"]
,   'final_idvars':['LOCATION','brand','date']
}


def maintain_dir(loc_dir=''):
    if os.path.exists(loc_dir):
        for f in os.listdir(loc_dir):
            try : 
                print('{a}/{b}'.format(a=loc_dir,b=f))
                os.remove('{a}/{b}'.format(a=loc_dir,b=f))
            except:
                print('ERROR: cannot be deleted: {}'.format('{a}/{b}'.format(a=loc_dir,b=f)))
    else: 
        os.mkdir(loc_dir)
        os.chmod(loc_dir,0o777)


download_loc = '{a}/{b}'.format(a=config.get('home_dir'),b=config.get('download_dir'))
staging_loc = '{a}/{b}'.format(a=config.get('home_dir'),b=config.get('staging_dir'))

maintain_dir(download_loc)
maintain_dir(staging_loc)



##############################
data_source = {
    ## PLATTS
    "platts_CB": {
        "source":'platts'
    ,   "feed":"https://pmc.platts.com/Login.aspx"
    ,   "how":"download_csv"    # could be email
    ,   "url":"https://pmc.platts.com/MQT/MQTHome.aspx?nl=Market%20Data%20Snapshot&nl2=Snapshot&nl4=More%20Products"
    }
,   "platts_RB": {
        "source":'platts'
    ,   "feed":"https://pmc.platts.com/Login.aspx"
    ,   "how":"download_csv"    # could be email
    ,   "url":"https://pmc.platts.com/MQT/MQTHome.aspx?nl=Market%20Data%20Snapshot&nl2=Snapshot&nl4=More%20Products"
    }
    ## Informed Sources
,   "is_capital": {
        "source":'informed_sources'
    ,   "feed":"190716-NRMA - Capital Cities Current Averages (9am)"
    ,   "how":"inbox_xls"    # could be email
    ,   "url":""
    }
,   "is_state": {
        "source":'informed_sources'
    ,   "feed":"190716-NRMA - State Daily Averages for Yesterday"
    ,   "how":"inbox_xls"    # could be email
    ,   "url":""
    }
,   "is_sydney": {
        "source":'informed_sources'
    ,   "feed":"190716-NRMA - Sydney Current Price Distribution (9am)"
    ,   "how":"inbox_xls"    # could be email
    ,   "url":""
    }
    ## RBA
,   "audusd": {
        "source":'rba'
    ,   "feed":"audusd"
    ,   "how":"download_csv"
    ,   "url":"https://www.rba.gov.au/statistics/tables/csv/f11.1-data.csv"
    }
# ,   "fuel_stations":{}
####################################
## TGP
,   "tgp_caltex":{
        "source":"caltex"
    ,   "feed":"tgp"
    ,   "how":"pdf"
    ,   "url":"https://www.caltex.com.au/-/media/pricing/caltex-terminal-gate-prices.ashx"
    
    }
,   "tgp_bp":{
        "source":"bp"
    ,   "feed":"tgp"
    ,   "how":"pdf"
    ,   "url":"https://www.bp.com/content/dam/bp-country/en_au/products-services/pricing/terminal-gate-price.pdf"
    }
,   "tgp_mobil":{
        "source":"mobil"
    ,   "feed":"tgp"
    ,   "how":"html"
    ,   "url":"http://apps.exxonmobil.com.au/apps/htm/mn_mobil_products_automotive_pricing.asp"
    }
,   "tgp_viva":{
        "source":"viva"
    ,   "feed":"tgp"
    ,   "how":"html"
    ,   "url":"https://www.vivaenergy.com.au/products/terminal-gate-pricing/current-tgp/tgp-current"
    }
,   "tgp_liberty":{
        "source":"liberty"
    ,   "feed":"tgp"
    ,   "how":"html"
    ,   "url":"https://www.libertyoil.com.au/terminal-gate-pricing"
    }
,   "tgp_puma":{
        "source":"puma"
    ,   "feed":"tgp"
    ,   "how":"html"
    ,   "url":"https://www.pumaenergy.com.au/for-business/terminal-gate-price/"
    }
,   "tgp_united":{
        "source":"united"
    ,   "feed":"tgp"
    ,   "how":"html"
    ,   "url":"https://www.unitedpetroleum.com.au/wholesale/tgp-pricing/"
    }
}









