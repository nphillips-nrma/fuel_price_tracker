## ensure enviroment variables are set: "AUTH_DIR", "FUEL_DIR"


import pandas as pd
import numpy as np
import re,time,os,sys
import time,datetime,math
from bs4 import BeautifulSoup
import hashlib 

os.system("/home/ec2-user/.bash_profile")
sys.path.append(os.environ.get('FUEL_DIR'))

from config import * 
from utils import * 

##
# print("STEP 1: (EXTRACT) download data")

sources = pd.Series(list(data_source.keys()))
tgp_sources = sources[sources.str.contains('tgp_\w+')].to_list()

for job_id in tgp_sources:
    print('job: {}'.format(job_id))
    job_params = data_source.get(job_id)
    # run job
    job_obj = pull_data(job_id,download_loc)
    job_obj.__run__()

###

print("STEP 2: (TRANSFORM) pull required data")

sources = pd.Series(list(data_source.keys()))
tgp_sources = sources[sources.str.contains('tgp_\w+')].to_list()

for job_id in tgp_sources:  # job_id = 'tgp_viva'
    print('job: {}'.format(job_id))
    # run job
    job_obj = transform_src(job_id,download_loc)
    job_obj.__run__()


print("STEP 3: (LOAD) final data")

i = 0 
while i <= 2 : 
    try:
        load_df = pd.DataFrame({"filename":os.listdir(staging_loc)})
        master_df = pd.DataFrame()
        for job_f in load_df.filename:
            print(job_f)
            job_df = pd.read_csv('{a}/{b}'.format(a=staging_loc, b=job_f))
            job_df['brand'] = re.search('\w+_(\w+)_\d+.csv',job_f).group(1)
            master_df = pd.concat([master_df,job_df], axis=0,ignore_index =True,sort=False)
        i = 3
    except:
        print('<ERROR> STEP 3 building master (While loop: try again) ')
        i += 1

master_df.LOCATION = master_df.LOCATION.str.replace("TGP","").str.strip()

#### generate final fields to add to PROD table
print("STEP 4: Create TGP summary")

master_df2 = master_df.melt(id_vars=config.get('final_idvars'),value_vars =config.get('final_fuelset') )
master_df2.LOCATION = master_df2.LOCATION.str.replace("TGP","").str.strip()

## keep latest date only 
master_df2 = master_df2.merge(
    master_df.groupby('brand').date.max().reset_index()
,   on=['date','brand'], how ='inner'
)

# filter for fuel types
master_df2 = master_df2.query('variable in {}'.format(str(config.get('final_fuelset')))
    ).groupby(list(np.setdiff1d(config.get('final_idvars'),['date']))+['variable']
    ).value.max(
    ).unstack('brand').reset_index()

# Only keep too locations
master_df2 =  master_df2.query('LOCATION in ["SYDNEY","NEWCASTLE"]')

## final table
update_dt = pd.to_datetime("1970-01-01 00:00:00 UTC") + pd.DateOffset(seconds=time.time())
master_df2['updated_dt'] = update_dt.tz_convert('Australia/Sydney').strftime("%Y-%m-%d %H:%M %p")

#### Load final table
dt = int(time.time())
staging_dir = '{a}/{b}_{c}.csv'.format(a=staging_loc, b='staging',c=dt)


# SHEET 1: generate final new prod TGP DF

print("STEP 5: UPLOAD TGP_SUMM data")

spreadsheet = gsheet_client.open_by_url('https://docs.google.com/spreadsheets/d/1r07-VhvVvGYVkcqEP3ZS1KkdT9xZW5HN5HRTVbjvI6U/edit?usp=sharing')

# list_of_lists = spreadsheet.sheet1.get_all_values()
# current_df = pd.DataFrame(list_of_lists[1:],columns=list_of_lists[0]) 
# current_df = current_df.query('dedup_hash not in {} '.format(list(master_df2.dedup_hash)))
# current_df = pd.concat([current_df,master_df2],axis=0,sort=False)
master_df2.to_csv(staging_dir,index=False)

prod_df = open(staging_dir,'r').read().encode('utf-8')
gsheet_client.import_csv(file_id = spreadsheet.id,data = prod_df)

# SHEET 2: generate final new prod TGP DF

print("STEP 6: UPLOAD TGP_BASE data")


## hash rows

tgp_base = master_df.melt(id_vars=config.get('final_idvars'),value_vars=config.get('final_fuelset') )
tgp_base = tgp_base.rename(columns={'variable':'fuel_type','value':'fuel_price'})
tgp_base = tgp_base.query('LOCATION==LOCATION')#bug in MOBIL data  
tgp_base['dedup_hash'] = tgp_base[config.get('final_idvars')+['fuel_type']].apply(lambda x: hashlib.md5('-'.join(x).encode()).hexdigest() , axis=1)
tgp_base = tgp_base.query('fuel_price == fuel_price')
tgp_base['upd_dt_utc'] = dt


## chekc date 
dt_range = (pd.to_datetime(tgp_base.date.max()) - pd.to_datetime(tgp_base.date.min())).days
assert dt_range < 7 , "ERROR: pulling through weird dates"

spreadsheet = gsheet_client.open_by_url('https://docs.google.com/spreadsheets/d/1df8SMAI-gOCYpNwTX1_v8ab0xY0hrLdPBzFvymaqoWc/edit?usp=sharing')

list_of_lists = spreadsheet.sheet1.get_all_values()
current_df = pd.DataFrame(list_of_lists[1:],columns=list_of_lists[0]) 
current_df = current_df.query('dedup_hash not in {} '.format(list(tgp_base.dedup_hash)))
current_df = pd.concat([current_df,tgp_base],axis=0,sort=False)
current_df.to_csv(staging_dir,index=False)

prod_df = open(staging_dir,'r').read().encode('utf-8')
gsheet_client.import_csv(file_id = spreadsheet.id,data = prod_df)


