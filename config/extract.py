"""
    OBJECTIVE:  This is the configuration to pull information required from raw data sources
    CREATOR:    Nathan Phillips
    DATE:       2019/07/22
"""

import numpy as np
import re 
import pandas as pd 

funcs_helpers = {
    "clean_str":lambda x: x.strip('[* ]').upper() if x.strip('[* ]').upper() not in ['','\xa0'] else np.NaN
,   "clean_float":lambda x: x if re.search('\d+.*\d*',str(x)) is not None else np.NaN
,   "clean_date":lambda x: pd.to_datetime(x,format='%d %B %Y').strftime('%Y-%m-%d')
}



extract_map = {
    "tgp_caltex":{
        "how":"pdf"
    ,   "identifier":"header" # this means index PDF text to column name 
    ,   "header":["State","Location","Previous","Current","Previous","Current"
                ,"Previous","Current","Previous","Current","Previous","Current"]
    ,   "first_col":"Effective Date"
    ,   "keep_colnames":False
    ,   'colnames':{    0:'State'       ,   1:'Location'
                    ,   2:'e10_prev'    ,   3:'e10_curr'
                    ,   4:'ULP_prev'    ,   5:'ULP_curr'
                    ,   6:'PULP_prev'   ,   7:'PULP_curr'
                    ,   8:'ULP98_prev'  ,   9:'ULP98_curr'
                    ,   10:'Diesel_prev',   11:'Diesel_curr'
                    }
    ,   "first_col":"State"
    ,   'find_dates':{'fmt':'%A, %d %B %Y'}
    ,   "validators":{
            "State":{"regex":'|'.join(["NSW","QLD","VIC","TAS","SA","NT","WA"])}
        ,   "Location":{"regex":"\w+"}
        }
    ,   "overrides":{
            "Location": {"f":funcs_helpers.get('clean_str')}
        ,   "Diesel":{"f":funcs_helpers.get('clean_float')}
        ,   "PULP":{"f":funcs_helpers.get('clean_float')}
        ,   "ULP":{"f":funcs_helpers.get('clean_float')}
        ,   "ULP98":{"f":funcs_helpers.get('clean_float')}
        ,   "e10":{"f":funcs_helpers.get('clean_float')}
        }
    ,   "rename":{
            "State":"STATE"
        ,   "Location":"LOCATION"
        ,   "Diesel":"DIESEL"
        ,   "e10":"E10"
        ,   "PULP":"PULP95"
        ,   "ULP98":"PULP98"
        }
    ,   "output_fields":['STATE','LOCATION','date','DIESEL','PULP95','ULP','PULP98','E10']
    }
,   "tgp_bp":{
        "how":"pdf"
    ,   "identifier":"header" # this means index PDF text to column name 
    ,   "header":["Effective Date","Terminal","Diesel","ULP","PULP","e10"]
    ,   "first_col":"Effective Date"
    ,   "keep_colnames":True
    ,   'colnames':{}
    ,   "validators":{
            "Effective Date":{"regex":"\d+ \w+ \d{4}"}
        }
    ,   "overrides":{
            "Effective Date":{"f":funcs_helpers.get('clean_date')}
        ,   "Terminal": {"f":funcs_helpers.get('clean_str')}
        ,   "Diesel":{"f":funcs_helpers.get('clean_float')}
        ,   "PULP":{"f":funcs_helpers.get('clean_float')}
        ,   "ULP":{"f":funcs_helpers.get('clean_float')}
        ,   "e10":{"f":funcs_helpers.get('clean_float')}
        }
    ,   "rename":{
            "Terminal":"LOCATION"
        ,   "Diesel":"DIESEL"
        ,   "e10":"E10"
        ,   "Effective Date":"date"
        ,   "PULP":"PULP95"
        }
    ,   "output_fields":['LOCATION','date','DIESEL','PULP95','ULP','E10']
    }
,   "tgp_mobil":{
        "how":"html"
    ,   "identifier":{"high":"div","low":{"id":"table-tgp"}}
    ,   'id_rows':{"high":"tr","low":{}}
    # ,   "columns":{"value":"th"}
    ,   "row":{"value":["td","th"]}
    ,   "value_head":0
    ,   "value_row":1
    ,   "overrides":{
            "state_FOUND": {"f":funcs_helpers.get('clean_str'),"na":"ffill"}
        ,   '':{"f":funcs_helpers.get('clean_str')}
        ,   "ULP":{"f":funcs_helpers.get('clean_float')}
        ,   "PULP":{"f":funcs_helpers.get('clean_float')}
        ,   "ULS DIESEL":{"f":funcs_helpers.get('clean_float')}
        ,   "E10":{"f":funcs_helpers.get('clean_float')}
        ,   "98R":{"f":funcs_helpers.get('clean_float')}
        }
    ,   "rename":{
            "state_FOUND":"STATE"
        ,   '':"LOCATION"
        ,   "ULS DIESEL":"DIESEL"
        ,   "98R": "PULP98"
        ,   "PULP":"PULP95"
        }
    ,   "find_date":{   
            'id':'p'
        ,   "regex":".* In Capital Cities As At (\d+ \w+ \d{4})"
        ,   "dt_fmt":"%d %B %Y"
        }
    ,   "output_fields":['STATE','LOCATION','date','DIESEL','ULP','PULP95','PULP98','E10']
    ,   "rename_value":{"LOCATION":{"BOTANY":"SYDNEY"}}
    }
,   "tgp_liberty":{
        "how":"html"
    ,   "identifier":{"high":"div","low":{"class":"col-md-12"}}
    ,   'id_rows':{"high":'div',"low":{"class":"row"}}
    ,   "row":{"value":["div"]}
    ,   "value_head":1
    ,   "value_row":2
    ,   "overrides":{
            "state_FOUND":{'f':funcs_helpers.get('clean_str'),"na":"ffill"}
        ,   "Location": {'f':funcs_helpers.get('clean_str')}
        ,   "ULS Diesel":{'f':funcs_helpers.get('clean_float')}
        ,   "Prem. Unleaded":{'f':funcs_helpers.get('clean_float')}
        ,   "Unleaded":{'f':funcs_helpers.get('clean_float')}
        }
    ,   "rename":{
            "Location":"LOCATION"
        ,   "ULS Diesel":"DIESEL"
        ,   "Unleaded":"ULP"
        ,   "Prem. Unleaded":"PULP95"
        ,   "state_FOUND":"STATE"
        }
    ,   "find_date":{   
            'id':'strong'
        ,   "regex":"Effective from (\d{1,2}/\d{1,2}/\d{4})"
        ,   "dt_fmt":"%d/%m/%Y"
        }
    ,   "output_fields":['STATE','LOCATION','date','DIESEL','PULP95','ULP']
    }
,   "tgp_puma":{
        "how":"html"
    ,   "identifier":{"high":"table","low":{'class':'small'}}
    ,   'id_rows':{"high":'tr',"low":{}}
    ,   "row":{"value":["td","th"]}
    ,   "value_head":0
    ,   "value_row":1
    ,   "overrides":{
            "Terminal":{'f':funcs_helpers.get('clean_str')}
        ,   "ULS Automotive Diesel":{'f':funcs_helpers.get('clean_float')}
        ,   "Unleaded Petrol 91":{'f':funcs_helpers.get('clean_float')}
        ,   "Premium Unleaded Petrol 95":{'f':funcs_helpers.get('clean_float')}
        ,   "Premium Unleaded Petrol 98":{'f':funcs_helpers.get('clean_float')}
        ,   "E10 Unleaded Petrol":{'f':funcs_helpers.get('clean_float')}
        }
    ,   "rename":{
            "Terminal":"LOCATION"
        ,   "ULS Automotive Diesel":"DIESEL"
        ,   "Unleaded Petrol 91":"ULP"
        ,   "Premium Unleaded Petrol 98":"PULP98"
        ,   "Premium Unleaded Petrol 95":"PULP95"
        ,   "E10 Unleaded Petrol":"E10"
        }
    ,   "find_date":{   
            'id':'strong'
        ,   "regex":"Pricing effective from (\d+ \w+ \d{4})"
        ,   "dt_fmt":"%d %B %Y"
        }
    ,   "output_fields":['LOCATION','date','DIESEL','PULP95',"PULP98",'ULP','E10']
    }
,   "tgp_united":{
        "how":"html"
    ,   "identifier":{"high":"table","low":{}}
    ,   'id_rows':{"high":'tr',"low":{}}
    ,   "row":{"value":["td"]}
    ,   "value_head":0
    ,   "value_row":1
    ,   'head_dt':{'fmt':'%d/%m/%Y','rename':'fuel','fmt_raw':'(\d+/\d+/\d{4})'}
    ,   'subh':{
            'field': 'Price Excluding GST' 
        ,   'filter':lambda x: x.strip()==''
        ,   'target':'fuel'
        ,   'subfilter':lambda x:'TGP' in x
        ,   'colname':'Location'
        }
    ,   'pivot':{'row':['date','Location'],'col':['fuel'],'value':'Price Excluding GST'}
    ,   "overrides":{
            "Location":{'f':funcs_helpers.get('clean_str')}
        ,   "Diesel":{'f':funcs_helpers.get('clean_float')}
        ,   "E10":{'f':funcs_helpers.get('clean_float')}
        ,   "Premium 95":{'f':funcs_helpers.get('clean_float')}
        ,   "Premium 98":{'f':funcs_helpers.get('clean_float')}
        ,   "Unleaded":{'f':funcs_helpers.get('clean_float')}
        }
    ,   "rename":{
            "Location":"LOCATION"
        ,   "Diesel":"DIESEL"
        ,   "Unleaded":"ULP"
        ,   "Premium 98":"PULP98"
        ,   "Premium 95":"PULP95"
        }
    ,   "output_fields":['LOCATION','date','DIESEL','PULP95',"PULP98",'ULP','E10']
    }
,   "tgp_viva":{
        "how":"html"
    # ,   "identifier":{"high":"div","low":{'class':'row'}}
    ,   'id_rows':{"high":'tr',"low":{}}
    ,   "row":{"value":"td"}
    ,   "value_row":1
    ,   "find_date":{   
            'id':'h2'
        ,   "regex":".* Terminal Gate Pricing \(TGP\) as at (\d+ \w+ \d{4})"
        ,   "dt_fmt":"%d %b %Y"
        }
    ,   "overrides":{
            "state_FOUND":{'f':funcs_helpers.get('clean_str'),"na":"ffill"}
        ,   1:{'f':funcs_helpers.get('clean_str')}
        ,   2:{'f':funcs_helpers.get('clean_float')}
        ,   3:{'f':funcs_helpers.get('clean_float')}
        ,   4:{'f':funcs_helpers.get('clean_float')}
        ,   5:{'f':funcs_helpers.get('clean_float')}
        ,   6:{'f':funcs_helpers.get('clean_float')}
        }
    ,   "rename":{
            'state_FOUND':"STATE"
        ,   1:"LOCATION"
        ,   2:"ULP"
        ,   3:"PULP95"
        ,   4:"E10"
        ,   5:"PULP98"
        ,   6:"DIESEL"
        }
    ,   "output_fields":['STATE','LOCATION','date','DIESEL','PULP95',"PULP98",'ULP','E10']
    }
}










