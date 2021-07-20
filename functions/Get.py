#bibliotecas
import numpy as np
import pandas as pd
import json
import requests
from datetime import date
import psycopg2
import sqlalchemy
from sqlalchemy import create_engine
import io
import sys

class Get():

    def __init__(self, token = '', store_type = 'csv', store_mode = 'trunc'):
        '''
        Generic Get class to make a get call to Pontomais API

        Atibutes:
            mode = 
        '''
        #API basic call attributes
        self.token = token
        self.header = {'access-token' : token}
        self.base_url = 'https://api.pontomais.com.br/external_api/v1/allowances?'

        #store attributes
        self.store_type = store_type
        self.store_mode = store_mode
        self.local_path = ''
        self.database = ''
        self.db_user = ''
        self.db_password = ''
        self.db_host = ''
        self.db_port = ''

    def set_token(self, token):
        self.token = token
        self.header = {'access-token' : token}

    def refresh_header(self):
        self.header = {'access-token' : token}

    def store(self, df, store_name):
        if self.store_type == 'csv':
            df.to_csv(self.local_path + '/' + store_name +'.csv', index = False)
            
        elif self.store_type == 'xlsx':
            df.to_excel(self.local_path + '/' + store_name +'.xlsx', index = False)
            
        elif self.store_type == 'postgres':
            if len(df) == 0:
                sys.exit('Empty Dataframe')
            else:
                engine_path = 'postgresql+psycopg2://'+ str(self.db_user) + ':' + str(self.db_password) + '@' + str(self.db_host) \
                            + ':' + str(self.db_port) + '/' + str(self.database) 
                engine = create_engine(engine_path)        
                con = engine.raw_connection()
                cur = con.cursor()
                
                
                if self.store_mode == 'trunc':
                    cur.execute('truncate ' + store_name +';')
                
                elif self.store_mode == 'create':
                    df.head(0).to_sql(store_name, engine, if_exists = 'replace', index = False)

                #faster than df.to_sql to input data    
                output = io.StringIO()
                df.to_csv(output, sep = '\t', header = False, index = False)
                output.seek(0)
                cur.copy_from(output, store_name, null = "") # null values become ''

                con.commit()
                con.close()

        elif self.store_type == 'mysql':
            if len(df) == 0:
                sys.exit('Empty Dataframe')
            else:
                engine_path = 'mysql+mysqldb://'+ str(self.db_user) + ':' + str(self.db_password) + '@' + str(self.db_host) \
                            + ':' + str(self.db_port) + '/' + str(self.database)  
                engine = create_engine(engine_path)        
                con = engine.raw_connection()

                if self.store_mode == 'trunc':
                    df.to_sql(store_name, con = engine, index = False, if_exists = 'replace')
                
                elif self.store_mode == 'create':
                    df.head(0).to_sql(store_name, engine, if_exists = 'replace', index = False)
                    
                con.commit()
                con.close()
            
        else:
            sys.exit('Invalid Option')


    def call_abonos(self, store_name, start_date, end_date, medical_certificate = ['true','false'], return_df = False):
        print('--------Call Abonos--------')
        url_base = self.base_url
        start_date = start_date
        end_date = end_date
        medical_certificate = medical_certificate
        
        df = pd.DataFrame()

        for i in range(len(medical_certificate)):
            print('--------Loop Medical Certificate--------')
            print('Medical Certificate = ' + str(medical_certificate[i]))
            #Concatenate the URL and call the API
            url = url_base + 'start_date=' + start_date + '&' + 'end_date=' + end_date + '&' + 'medical_certificate=' + str(medical_certificate[i])
            r = requests.get(url, headers = self.header).json()

            #parse the result into a dataframe and concatenate the loop calls into one dataframe
            df_temp = pd.json_normalize(r['exemptions'])
            df_temp['observation'] = df_temp['observation'].str.replace(' \n',' ')
            df_temp['answered_by.team.leader_ids'] = [', '.join(map(str, l)) for l in df_temp['answered_by.team.leader_ids']]
            df = pd.concat([df,df_temp],ignore_index=True)
            df = df_temp

        self.store(df, store_name)
            
        if return_df:
            result = df            
        else:
            result = None
        
        return result