
import pandas as pd 
import numpy as np



contract_dict = {
    'columns':['State','Location','date','PULP','ULP','E10','Diesel']
,   'optional':['ULP98']
,   'State':['NSW','NT','QLD','SA','TAS','VIC','WA']
,   'Location':['Sydney']
}

contract_test = {
    'table':{
        'requied':{
            'type':'isin'
        ,   'test':'no_nulls'
        ,   'values':contract_dict.get('keep')
        }
    }
,   'col':{
            'State':{
                'type':'isin'
            ,   'test':'no_nulls'
            ,   'values':contract_dict.get('State')
            }   
        ,   'Location':{
                'type':'isin'
            ,   'test':'gt0'
            ,   'values': contract_dict.get('location')
            }
        ,   'date':{
                'type':'datetime'
            ,   'test':'no_nulls'
            ,   'values':"%d/%m/%Y"
            }
        ,   'PULP':{
                'type':'is_numeric'
            ,   'test':'gt0'
        }
        ,   'ULP':{
                'type':'is_numeric'
            ,   'test':'gt0'
        }
        ,   'E10':{
                'type':'is_numeric'
            ,   'test':'gt0'
        }
        ,   'Diesel':{
                'type':'is_numeric'
            ,   'test':'gt0'
        }
    }
}



class validate_tgp: 
    def __init__(self,df,contract_test):
        self.df = df
        self.contract_test = contract_test
    
    def __test_isin(self,colname=''):
        test_id = self.contract_test.get('col')
        output = self.df[colname].isin(test_id.get(colname))
        if test_id.get('test') == 'gt0':
            return output.sum()> 0 
        elif test_id.get('test') == 'no_nulls':
            return (output==False).sum() == 0 
    
    # def __test_numeric(self,colname=''):
    #     test_id = self.contract_test.get('col')
    #     self.df[colname].apply(lambda x: )
    
    def __test_datetime(self,colname=''):
        test_id = self.contract_test.get('col')
        return pd.to_datetime(self.df[colname],test_id.get('values')).isnull().sum() == 0 
    
    def run_tests(self):
        test_results = []
        ## high level table testing
        test_results.append({'table':np.setdiff1d(self.contract_dict,list(self.df.columns))==0})
        ## loop through column testing
        for col in self.contract_test.keys():
            test_id = self.contract_test.get('col').get(col)
            if test_id.get('type') == 'isin':
                test_results.append({col:__test_isin(col)})
            if test_id.get('type') == 'datetime':
                test_results.append({col:__test_datetime(col)})
        return test_results



