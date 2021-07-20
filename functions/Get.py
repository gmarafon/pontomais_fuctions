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

class Get:
    '''
    Generic Get class to make get calls to Pontomais API

    Args:
        token: authentication token. Default = ''
        store_type: csv, xlsx, postgres, mysql. Default = csv
        store_mode: trunc, create. Default = trunc

    Other Atributes:
        header: header passed to the API
        local_path: path to store the returned files
        database: database name
        db_user: database user
        db_password: database user password
        db_host: database host
        db_port: database port
    '''

    def __init__(self, token = '', store_type = 'csv', store_mode = 'trunc'):
        #API basic call attributes
        self.token = token
        self.header = {'access-token' : token}

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
        '''
        Function to set the token value and refresh the header to the new token

        Args:
            token: new authorization token

        Returns:
            None
        '''
        self.token = token
        self.header = {'access-token' : token}

    def refresh_header(self):
        '''
        Function to refresh the header

        Args:
            None

        Returns:
            None
        '''
        self.header = {'access-token' : token}

    def _store(self, df, store_name):
        '''
        Function to store the dataframe returned through a call function accordingly to store_type and 
        store_mode

        Args:
            df: dataframe returned through the call function
            store_name: file or table name to store the dataframe

        Returns:
            None
        '''
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
        '''
        Function to call the Abonos API and store the return

        Args:
            store_name: file or table name to store the dataframe
            start_date: 'YYYY-MM-DD' format. Start date passed to the API
            end_date: 'YYYY-MM-DD' format. End date passed to the API
            medical_certificate: list containing 'true' and/or 'false'. Default ['true','false']
            return_df: boolean to set return or not the dataframe from the API. Default False

        Returns:
            DataFrame or None
        '''
        print('--------Call Abonos--------')
        url_base = 'https://api.pontomais.com.br/external_api/v1/allowances?'
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

        self._store(df, store_name)
            
        if return_df:
            result = df            
        else:
            result = None
        
        return result

    def call_afastamentos(self, store_name, return_df = False):
        '''
        Function to call the Afastamentos API and store the return

        Args:
            store_name: file or table name to store the dataframe
            return_df: boolean to set return or not the dataframe from the API. Default False

        Returns:
            DataFrame or None
        '''
        print('--------Get Afastamentos--------')
        #set the url and call the API
        url_base = 'https://api.pontomais.com.br/external_api/v1/absences'
        url = url_base
        r = requests.get(url, headers = self.header).json()

        #parse the json
        df_temp = pd.json_normalize(r['absences'])
        df = df_temp

        self._store(df, store_name)
        
        if return_df:
            result = df
        else:
            result = None
        
        return result

    def get_banco_horas(self, store_name, employee_id, withdraw = ['true', 'false'], return_df = False):
        '''
        Function to call the Banco de Horas API and store the return

        Args:
            store_name: file or table name to store the dataframe
            employee_id: list of employees id to pass to the API
            withdraw: list containing 'true' and/or 'false'. Default ['true','false']
            return_df: boolean to set return or not the dataframe from the API. Default False
            
        Returns:
            DataFrame or None
        '''
    
        print('--------Get Banco de Horas--------')
        url_base = 'https://api.pontomais.com.br/external_api/v1/time_balance_entries?count=true&sort_property=date&sort_direction=desc&attributes=id,date,withdraw,amount,employee_id,observation,updated_by&'
        page = 1
        per_page = 100
        df = pd.DataFrame()

        #loop withdraw
        print('--------Loop Withdraw--------')
        for i in range(len(withdraw)):
            print('Withdraw = ' + str(withdraw[i]))

            print('--------Loop Employee--------')
            for j in range(len(employee_id)):     
                print('id = ' + str(employee_id[j]))
                
                #set the url and call the API
                url = url_base + 'page=' + str(page) + '&' + 'per_page=' + str(per_page) + '&' + 'employee_id=' + str(employee_id[j]) + '&' + 'withdraw=' + str(withdraw[i])
                r = requests.get(url, headers = self.header).json()

                #parse the json and concatenate the dataframes
                df_temp = pd.json_normalize(r['time_balance_entries'])
                df = pd.concat([df, df_temp], ignore_index=True)

        self.store(df, store_name)
        
        if return_df:
            result = df
        else:
            result = None
        
        return result

    def get_centro_custo(self, store_name, return_df = False):
        '''
        Function to call the Centro de Custo API and store the return

        Args:
            store_name: file or table name to store the dataframe
            return_df: boolean to set return or not the dataframe from the API. Default False
            
        Returns:
            DataFrame or None
        '''
    
        print('--------Get Centro de Custos--------')
        #set the url and call the API
        url_base = 'https://api.pontomais.com.br/external_api/v1/cost_centers'
        url = url_base
        r = requests.get(url, headers = self.header).json()
        
        #parse the json
        df_temp = pd.json_normalize(r['cost_centers'])
        df = df_temp[['id','code','name']]

        self.store(df, store_name)
        
        if return_df:
            result = df_cc
        else:
            result = None
        
        return result

#TODO
    def get_cidade(mode,name, retorno,table_mode):
    
        print('--------Get Cidades--------')
        #Cidades
        url_base = 'https://api.pontomais.com.br/external_api/v1/cities?attributes=id,name&name=curitiba&sort_direction=asc&count=true&'
        page = 1
        per_page = 100
        url = url_base + 'page=' + str(page) + '&' + 'per_page=' + str(per_page)
        #print(url)

        r = requests.get(url, headers = parameters.header).json()

        df_temp = pd.json_normalize(r['cities'])
        df_ci = df_temp[['id','name','state']]

        store(df_ci, name, mode, table_mode)
        
        if retorno:
            result = df_ci
        else:
            result = None
        
        return result

    def get_colaboradores(mode,name, retorno,table_mode):
        
        print('--------Get Colaboradores--------')
        #Colaboradores
        url_base = 'https://api.pontomais.com.br/external_api/v1/employees?active=true&attributes=id,first_name,last_name,email,pin,is_clt,cpf,nis,registration_number,time_card_source,has_time_cards,use_qrcode,enable_geolocation,work_hours,cost_center,user,enable_offline_time_cards,login&count=true&sort_direction=asc&sort_property=first_name&'
        page = 1
        per_page = 100
        url = url_base + 'page=' + str(page) + '&' + 'per_page=' + str(per_page)
        #print(url)

        r = requests.get(url, headers = parameters.header).json()
        df_temp = pd.json_normalize(r['employees'])
        df_co = df_temp[['id','first_name','last_name','email','is_clt','user.id','user.active','user.confirmed_at']]
        df_co = df_co.rename(columns={'user.id': 'user_id','user.active':'active','user.confirmed_at':'confirmed_at'})
        df_co['full_name'] = df_co['first_name'] + ' ' + df_co['last_name']

        store(df_co, name, mode, table_mode)
        
        if retorno:
            result = df_co
        else:
            result = None
        
        return result

    def get_departamento(mode,name, retorno,table_mode):
        
        print('--------Get Departamentos--------')
        #Departamentos
        url_base = 'https://api.pontomais.com.br/external_api/v1/departments'
        url = url_base
        #print(url)

        r = requests.get(url, headers = parameters.header).json()
        r
        df_temp = pd.json_normalize(r['departments'])
        df_dp = df_temp[['id','code','name','employees_count']]

        store(df_dp, name, mode, table_mode)
        
        if retorno:
            result = df_dp
        else:
            result = None
        
        return result

    def get_excecoes_jornada(mode,name, retorno,table_mode):
        
        print('--------Get Exceções de Jornada--------')
        #Exceções de Jornada
        url_base = 'https://api.pontomais.com.br/external_api/v1/exemptions?'
        start_date='2020-01-01'
        medical_certificate = ['true','false']
        end_date= today.strftime('%Y-%m-%d')
        df_ej = pd.DataFrame()

        for i in range(len(medical_certificate)):
            print('--------Loop Certificado Médico--------')
            print('Certificado médico = ' + str(medical_certificate[i]))
            url = url_base + 'start_date=' + start_date + '&' + 'end_date=' + end_date + '&' + 'medical_certificate=' + str(medical_certificate[i])

            r = requests.get(url, headers = parameters.header).json()

            df_temp = pd.json_normalize(r['exemptions'])
            df_temp['observation'] = df_temp['observation'].str.replace(' \n',' ')
            df_temp['answered_by.team.leader_ids'] = [', '.join(map(str, l)) for l in df_temp['answered_by.team.leader_ids']]
            df_ej = pd.concat([df_ej,df_temp],ignore_index=True)
            df_ej = df_temp

        store(df_ej, name, mode, table_mode)
        
        if retorno:
            result = df_ej
        else:
            result = None
        
        return result

    def get_feriados(mode,name, retorno,table_mode):
        
        print('--------Get Feriados--------')
        #Feriados
        url_base = 'https://api.pontomais.com.br/external_api/v1/holidays?attributes=id,name,fixed,date,active,team,department,business_unit,cost_center,shift&count=true&'
        page = 1
        per_page = 100
        url = url_base + 'page=' + str(page) + '&' + 'per_page=' + str(per_page)
        #print(url)

        r = requests.get(url, headers = parameters.header).json()

        df_temp = pd.json_normalize(r['holidays'])
        df_fe = df_temp[['id','name','date','team','department','business_unit','cost_center']]

        store(df_fe, name, mode, table_mode)
        
        if retorno:
            result = df_fe
        else:
            result = None
        
        return result

    def get_gestores(mode,name, retorno,table_mode):
        
        print('--------Get Gestores--------')
        #Gestores
        url_base = 'https://api.pontomais.com.br/external_api/v1/possible_leaders?count=true&'
        page = 1
        per_page = 100
        url = url_base + 'page=' + str(page) + '&' + 'per_page=' + str(per_page)
        #print(url)

        r = requests.get(url, headers = parameters.header).json()

        df_temp = pd.json_normalize(r['leaders'])
        df_ge = df_temp[['id','name']]

        store(df_ge, name, mode, table_mode)
        
        if retorno:
            result = df_ge
        else:
            result = None
        
        return result

    def get_grupo_acesso(mode,name, retorno,table_mode):
        
        print('--------Get Grupos de Acesso--------')
        #Grupos de Acesso
        url_base = 'https://api.pontomais.com.br/external_api/v1/users/groups?attributes=id,name'
        url = url_base
        #print(url)

        r = requests.get(url, headers = parameters.header).json()
        r
        df_temp = pd.json_normalize(r['groups'])
        df_ga = df_temp[['id','name']]

        store(df_ga, name, mode, table_mode)
        
        if retorno:
            result = df_ga
        else:
            result = None
        
        return result


    def get_unidade_negocio(mode,name, retorno,table_mode):

        print('--------Get Unidade de Negócio--------')
        #Unidade de Negócio
        url_base = 'https://api.pontomais.com.br/external_api/v1/business_units?'
        page = 1
        per_page = 100
        url = url_base + 'page=' + str(page) + '&' + 'per_page=' + str(per_page)
        #print(url)

        r = requests.get(url, headers = parameters.header).json()
        df_temp = pd.json_normalize(r['business_units'])
        df_un = df_temp[['id','code','name']]

        store(df_un, name, mode, table_mode)
        
        if retorno:
            result = df_un
        else:
            result = None
        
        return result

    def get_usuarios(mode,name, retorno,table_mode):
        
        print('--------Get Usuários--------')
        #Usuários
        url_base = 'https://api.pontomais.com.br/external_api/v1/users?attributes=id,group,employee,sign_in_count,last_sign_in_at,last_sign_in_ip,confirmed_at,active,admin'
        url = url_base

        r = requests.get(url, headers = parameters.header).json()

        df_temp = pd.json_normalize(r['users'])
        df_us = df_temp

        store(df_us, name, mode, table_mode)
        
        if retorno:
            result = df_us
        else:
            result = None
        
        return result