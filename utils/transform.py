
import pandas as pd
import numpy as np
import re,time,os,sys
from bs4 import BeautifulSoup
from bs4.element import Tag
import PyPDF2

sys.path.append(os.environ.get('FUEL_DIR'))

from config import * 


def html_get_value(domType, classValue, x,idx=0):
    dummy = '<i href="missing" src="missing" content="missing" >'
    dummy = BeautifulSoup(dummy)
    dummy = dummy.findAll('i')[0]
    result = x.findAll(domType, classValue)
    if len(result) == 0:
        return(dummy)
    else:
        return(result[idx])


def html_get_text(html='<h1>hello world</h1>'): 
    try: 
        html_value = html.get_text()
    except: 
        html_value = np.NaN
    return html_value


class transform_src:
    def __init__(self,job_id,download_loc):
        self.job_id = job_id
        self.job_params = data_source.get(job_id)
        self.job_plan = extract_map.get(job_id)
        # run job
        download_df = pd.DataFrame({'filename':os.listdir(download_loc)})
        download_df['target'] = download_df.filename.str.contains(job_id)
        download_df['dt'] = download_df.filename.str.extract('_(\d+).',expand=False)
        # find latest file in download directory to pull extract data for
        self.target_df = download_df.query('target==1')
        self.target_df = self.target_df[self.target_df.dt == self.target_df.dt.max()]
        self.target_dir = '{a}/{b}'.format(a=download_loc,b=self.target_df.filename.iloc[0])
    #
    def transform_pdf(self ):
        print("<TRANSFORM_PDF> Loading PDF")
        with open(self.target_dir, 'rb') as pdfFileObj: 
            pdfReader = PyPDF2.PdfFileReader(pdfFileObj)
            total_pages = pdfReader.numPages
            ## pull PDF raw
            pdf_extract = []
            for page_i in range(total_pages):
                print('Page: {}'.format(page_i))
                pageObj = pdfReader.getPage(page_i)
                pdf_txt = pageObj.extractText()
                pdf_txt = pdf_txt.split('\n')
                pdf_extract +=pdf_txt
        #
        pdf_df = pd.DataFrame({'txt':pdf_extract})
        print("Loading PDF")
        # exec plan
        if self.job_plan.get('identifier') == "header":
            # helpers
            tab_col = len(self.job_plan.get('header') )
            max_range = len(pdf_extract)-len(self.job_plan.get('header') )
            # tabularise 
            aaa = pd.Series([ tab_col-len(np.setdiff1d(pdf_df.loc[range(x,x+tab_col),'txt'].to_list(),self.job_plan.get('header'))) for x in range(max_range)])
            pdf_df = pd.concat([pdf_df,aaa],axis= 1,sort=False).rename(columns={0:'col_match'}).fillna(0).reset_index()
            pdf_df['header'] = (pdf_df.txt.str.strip() == self.job_plan.get('first_col')).fillna(0)
            pdf_df['tableID'] = pdf_df.header.cumsum()
            table_ref = pdf_df.query('header==True').copy()
            pdf_df = pdf_df.drop(['col_match'],axis=1).merge(table_ref[['tableID','col_match']],on='tableID',how='left')
            # CHECK 1: atleast TABLE fOUnd
            assert pdf_df.tableID.max() > 0 , '<TRANSFORM_PDF> ERROR: PDF EXTRACT 1... no TABLES FOUND!!'
            # identity the Row/Column for the tables for re-shaping
            pdf_row_df = pdf_df.query('tableID>0').groupby(['tableID','col_match']).index.describe()[['min','max']].reset_index()
            pdf_row_df['rows'] = pdf_row_df.apply(lambda x: '|'.join([str(x) for x in range(int(x['min']),int(x['max']),int(x['col_match']))]),axis=1)
            pdf_row_df.index = pdf_row_df.tableID
            pdf_row_df = pdf_row_df.rows.str.split('|',expand=True).reset_index().melt(id_vars='tableID').drop(['variable'],axis=1).query('value==value')
            pdf_row_df['row_s'] =1
            pdf_row_df.value = pdf_row_df.value.astype(int)
            pdf_row_df = pdf_row_df.sort_values(['tableID','value'])
            pdf_row_df.row_s = pdf_row_df.groupby('tableID').cumcount()
            pdf_row_df = pdf_row_df.rename(columns={'value':'index'})
            assert pdf_row_df.groupby('tableID').row_s.max().min() > 0, "<TRANSFORM_PDF> ERROR: PDF EXTRACT 2: no row index found "
            ## identify and flip rows
            pdf_df = pdf_df.merge(pdf_row_df,on=['tableID','index'], how='left')
            pdf_df['rowID'] = pdf_df.row_s.fillna(method='ffill')
            keep_tab_row = pdf_df.groupby(['tableID','col_match','rowID']).size().reset_index().rename(columns={0:'n'})
            keep_tab_row = keep_tab_row.query('n==col_match')
            pdf_df = pdf_df.merge(keep_tab_row.drop(['n'],axis=1),on=['tableID','col_match','rowID'],how='inner')
            assert pdf_df.groupby(['tableID','rowID']).size().unstack('tableID').nunique(axis=0).max() == 1   , "<TRANSFORM_PDF> ERROR: PDF EXTRACT 3: Row * Column mapping is uneven "
            ## map column and CREATE TABLES
            pdf_df['colID'] = pdf_df.groupby(['tableID','rowID']).cumcount()
            ## if 
            if self.job_plan.get('keep_colnames'):
                print('<TRANSFORM_PDF> keep_colnames ACTIVATED')
                colname_df = pdf_df.query('rowID==0' ).copy()
                pdf_df = pdf_df.query('rowID>0').merge(
                    colname_df[['tableID','colID','txt']].rename(columns={'txt':'colname'})
                    ,on=['tableID','colID'], how='left')
                pdf_df = pdf_df.drop(['colID'],axis=1).rename(columns={'colname':'colID'})
            ## do the pivot
            pdf_tables = pdf_df.query('rowID >= 1').groupby(['tableID','rowID','colID']).txt.max().unstack('colID').reset_index()
            pdf_tables = pdf_tables.rename(columns=self.job_plan.get('colnames'))
            ## do checks
            if self.job_plan.get('validators') is not None:
                print('<TRANSFORM_PDF> validation ACTIVE')
                for col in self.job_plan.get('validators').keys():  # col = 'Effective Date'
                    pdf_tables = pdf_tables[pdf_tables[col].str.contains(self.job_plan.get('validators').get(col).get('regex'))]
            # 
            assert pdf_tables.shape[1] > 0 , '<TRANSFORM_PDF> ERROR: Generated a TABLE with NO ROWS'
            #
            if self.job_plan.get('find_dates') is not None:
                print('<TRANSFORM_PDF> find_dates ACTIVATED')
                dt_fields = pd.to_datetime(pdf_df.txt,format=self.job_plan.get('find_dates').get('fmt'),errors='coerce')
                df_map = pd.DataFrame([{'dt_type':'prev','date':dt_fields.min()},{'dt_type':'curr','date':dt_fields.max()}])
                # melt table down, append date for 'curr' vs 'prev' so the rows are identifiable
                pdf_tables2 = pdf_tables.melt(id_vars =['tableID','rowID','State','Location'])
                pdf_tables2['dt_type'] = pdf_tables2.colID.str.extract('_(\w+)',expand=False)
                pdf_tables2['colID'] = pdf_tables2.colID.str.extract('(\w+)_[prev|cuorr]',expand=False)
                pdf_tables2 = pdf_tables2.merge(df_map,on='dt_type',how='left')
                pdf_tables = pdf_tables2.groupby(['tableID','State','Location','rowID','date','colID']).value.max().unstack('colID').reset_index()
            # 
            if self.job_plan.get('overrides') is not None: 
                print('<TRANSFORM_PDF> overrides ACTIVATED')
                for col in self.job_plan.get('overrides').keys():
                    print(col)
                    pdf_tables[col] = pdf_tables[col].apply(self.job_plan.get('overrides').get(col).get('f'))
            #
            if self.job_plan.get('rename') is not None: 
                print('<TRANSFORM_PDF> rename columns ACTIVATED')
                pdf_tables = pdf_tables.rename(columns = self.job_plan.get('rename'))
            # 
            if self.job_plan.get('output_fields') is not None:
                print('<TRANSFORM_PDF> confirm field MVP ACTIVATED')
                assert len(np.setdiff1d(self.job_plan.get('output_fields'),pdf_tables.columns))== 0 ,'<TRANSFORM_PDF> ERROR missing fields {}'.format('|'.join(np.setdiff1d(self.job_plan.get('output_fields'),pdf_tables.columns)))
                pdf_tables = pdf_tables[np.intersect1d(self.job_plan.get('output_fields'),pdf_tables.columns)]
            #
            pdf_tables.to_csv('{a}/{b}.csv'.format(a=staging_loc,b=self.target_df.filename.str.replace('\\.\w+','').iloc[0]),index=False)
            return pdf_tables
        else: 
            print("<TRANSFORM_PDF> ERROR: could not find PDF METHOD: '{}'".format(self.job_plan.get('identifier') or "MISSING"))

    def transform_html(self ):
        print("<transform_html> Processing HTML")
        ## pull out raw text
        html_raw = open(self.target_dir,"r").read()
        html_soup = BeautifulSoup(html_raw, "html.parser")
        #  Pull out raw tables from HTML
        table_map = self.job_plan.get('identifier')
        if table_map is not None : 
            tables_raw = html_soup.findAll(table_map.get('high'),table_map.get('low'))
        else: 
            tables_raw = [html_soup]
        #
        tables_raw = pd.Series(tables_raw)
        ## kill off bad table identification
        tables_raw = tables_raw[tables_raw.apply(lambda x: x not in ['\n'])]
        # extract table
        id_rows = self.job_plan.get('id_rows')
        tables_list = [] 
        for idx,tb_i in enumerate(tables_raw): #tb_i=tables_raw.iloc[0]
            print('Table number: {}'.format(idx))
            raw_rows = []
            for row in tb_i.findAll(id_rows.get('high'),id_rows.get('low')):
                cells = row.findAll(self.job_plan.get('row').get('value'))
                # print([html_get_text(x) for x in cells])
                row_extract = list([html_get_text(x) for x in cells])
                # print(row_extract)
                raw_rows.append(row_extract)
            # formalise table
            if raw_rows == [] or (self.job_id =='tgp_liberty' and idx > 0): # bad manual hack :(
                print("WARNING: empty LIST, going to SKIP")
                continue
            # initialse table
            raw_df = pd.DataFrame(raw_rows)
            # apply columns names
            if self.job_plan.get('value_head') is not None:
                proposed_cols = raw_df.loc[self.job_plan.get('value_head')].str.strip().to_list()
                raw_df.columns = proposed_cols
            raw_df = raw_df.loc[self.job_plan.get('value_row'):]
            ## rename  empty state column 
            state_fields = ['NSW','VIC','QLD',"NEW SOUTH WALES","QUEENSLAND","VICTORIA"]
            find_state = raw_df.apply(lambda x: x.str.strip(),axis=0).isin(state_fields).sum(axis=0)>0
            if find_state.sum() > 0:
                proposed_cols = pd.Series(find_state.index)
                proposed_cols[find_state.values] = 'state_FOUND'
                raw_df.columns = proposed_cols
            ## date is the column 
            if self.job_plan.get('head_dt') is not None : 
                # print(raw_df)
                colnames = pd.Series(raw_df.columns)
                dt_colname = colnames[pd.to_datetime(colnames.str.extract(self.job_plan.get('head_dt').get('fmt_raw'),expand = False)
                    ,   format = self.job_plan.get('head_dt').get('fmt'),errors='coerce').notnull()]
                raw_df['date'] = pd.to_datetime(dt_colname,format = self.job_plan.get('head_dt').get('fmt') ).iloc[0]
                raw_df = raw_df.rename(columns = {dt_colname.iloc[0]:self.job_plan.get('head_dt').get('rename')} )
            # 
            if self.job_plan.get('find_date') is not None: 
                dt_short_list = pd.Series([x.get_text() for x in html_soup.findAll(self.job_plan.get('find_date').get('id'))]) 
                dt_short_list = dt_short_list.str.extract(self.job_plan.get('find_date').get('regex'),expand=False)
                raw_df['date'] = pd.to_datetime(dt_short_list,format=self.job_plan.get('find_date').get('dt_fmt')).max().strftime('%Y-%m-%d')
            ## column has both (1) FUEL and (2) Location this STEP will solve
            if self.job_plan.get('subh') is not None : 
                sh_filter = raw_df[self.job_plan.get('subh').get('field')].apply(self.job_plan.get('subh').get('filter'))
                req_col = raw_df.loc[sh_filter,self.job_plan.get('subh').get('target')]
                req_col = req_col[req_col.apply(self.job_plan.get('subh').get('subfilter'))]
                req_col.name = self.job_plan.get('subh').get('colname')
                raw_df = pd.concat([req_col,raw_df],axis=1,sort=False)
                raw_df[self.job_plan.get('subh').get('colname')] = raw_df[self.job_plan.get('subh').get('colname')].fillna(method='ffill')
                raw_df = raw_df[sh_filter==False]
            ## when table is Location * fuel_type unique, and want to pivot Fuel_type to column
            if self.job_plan.get('pivot') is not None: 
                pivot_i = self.job_plan.get('pivot')
                raw_df = raw_df.groupby(pivot_i.get('row')+pivot_i.get('col'))[pivot_i.get('value')].max().unstack(pivot_i.get('col')).reset_index()
            #
            print(raw_df)
            if self.job_plan.get('overrides') is not None: 
                print('<TRANSFORM_HTML> overrides ACTIVATED')
                for col in self.job_plan.get('overrides').keys():
                    print(col)
                    raw_df[col] = raw_df[col].fillna(value='').apply(self.job_plan.get('overrides').get(col).get('f'))
                    if self.job_plan.get('overrides').get(col).get('na') is not None:
                        raw_df[col] = raw_df[col].fillna(method=self.job_plan.get('overrides').get(col).get('na'))
            #
            if self.job_plan.get('rename') is not None: 
                print('<TRANSFORM_HTML> rename columns ACTIVATED')
                raw_df = raw_df.rename(columns = self.job_plan.get('rename'))
            if self.job_plan.get('rename_value') is not None:
                for col in self.job_plan.get('rename_value').keys():
                    print(col)
                    raw_df[col] = raw_df[col].replace(to_replace=self.job_plan.get('rename_value').get(col))
            # 
            if self.job_plan.get('output_fields') is not None:
                print('<TRANSFORM_HTML> confirm field MVP ACTIVATED')
                assert len(np.setdiff1d(self.job_plan.get('output_fields'),raw_df.columns))== 0 ,'<TRANSFORM_PDF> ERROR missing fields {}'.format('|'.join(np.setdiff1d(self.job_plan.get('output_fields'),raw_df.columns)))
                raw_df = raw_df[np.intersect1d(self.job_plan.get('output_fields'),raw_df.columns)]
                # add to master table list
                tables_list += [raw_df]
                # end for loop aggregate
        tables_output = pd.concat(tables_list, axis=0,sort=False,ignore_index=True)
        tables_output.to_csv('{a}/{b}.csv'.format(a=staging_loc,b=self.target_df.filename.str.replace('\\.\w+','').iloc[0]),index=False)
        return tables_output

    def __run__(self):
        print('job: {}'.format(self.job_id)) # job_id = 'tgp_caltex'
        if self.job_params.get('how') == "pdf":
            print("<__run__> EXTRACT PDF: '{}'".format(self.job_params.get('how')))    
            output_df = self.transform_pdf()
        elif self.job_params.get('how') == "html":
            print("<__run__> EXTRACT HTML: '{}'".format(self.job_params.get('how')))    
            output_df = self.transform_html()
        else: 
            print("<__run__> ERROR: could not find METHOD: '{}'".format(self.job_params.get('how')))    

    






