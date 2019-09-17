

import requests 
import re,time,os,sys
from bs4 import BeautifulSoup
from bs4.element import Tag

sys.path.append(os.environ.get('FUEL_DIR'))

from config import * 


## download csv

def download_csv(url=''):
    return


## Pull api

class pull_data: 
    def __init__(self,job_id,download_loc):
        self.job_id=job_id
        self.job_params = data_source.get(job_id)
        self.download_loc = download_loc
        self.dt = int(time.time())
    #
    def get_request(self):
        # get html from 'data_source' (config.py)
        self.html_raw = requests.get(self.job_params.get('url'))
        assert self.html_raw.status_code == 200 ,"ERROR: failed get request:'{}'".format(job_id)
        # return html_raw
    #
    # def get_soup(self):
    #     self.html_soup = BeautifulSoup(self.html_raw.text,features="html.parser") 
    #     # return html_soup
    # 
    def dump_content(self,fmt):
        with open('{a}/{b}_{c}.{d}'.format(a=self.download_loc,b=self.job_id,c=self.dt,d=fmt), 'wb') as zip_dump: 
            zip_dump.write(self.html_raw.content)
    # 
    # 
    # def extract_content(self):
    #     # extract
    #     extract_dir = '{a}/{b}'.format(a=payload_dir,b=filei)
    #     os.mkdir(extract_dir)
    #     with zipfile.ZipFile(output_dir, 'r') as zip_ref: 
    #         zip_ref.extractall(extract_dir)
    #     os.remove(output_dir)
    #
    def __run__(self):
        if self.job_params.get('how') in ('pdf','html'):
            self.get_request()
            self.dump_content(fmt=self.job_params.get('how'))
        else:
            print("ERROR: no JOB DESC for JOB='{}'".format(self.job_params.get('how')))



