import pandas as pd
import requests
from sqlalchemy import create_engine
from bs4 import BeautifulSoup
import regex as re
import numpy as np
import modin.pandas as mp

path = '/home/emmanuel/Desktop/descarga_db/raw_data_project_m1.db'
engine = create_engine(f'sqlite:///{path}')
raw_df = pd.read_sql_query("SELECT * FROM sqlite_master WHERE type='table'", engine)
df_personal = pd.read_sql('personal_info', engine)
df_country = pd.read_sql('country_info', engine)
df_career = pd.read_sql('career_info', engine)
df_poll = pd.read_sql('poll_info', engine)


#acquisitions
def get_dataframes(path:str):

    print('Getting tables...')
    df_personal = pd.read_sql('personal_info', engine)
    df_personal.to_csv('./raw_data/df_personal.csv')
    df_country = pd.read_sql('country_info', engine)
    df_country.to_csv('./raw_data/df_country.csv')
    df_career = pd.read_sql('career_info', engine)
    df_career.to_csv('./raw_data/df_career.csv')
    df_poll = pd.read_sql('poll_info', engine)
    df_poll.to_csv('./raw_data/df_poll.csv')

    engine.dispose()
    print('Engine stopped.')


    print('Dataframes downloaded and saved in folder.')
    return

def api_caller(x):

    print('getting job names...')
    req = []
    r = requests.get('http://api.dataatwork.org/v1/jobs/' + str(x))
    req = r.json()
    return req['normalized_job_title']




def get_jobs(normalized_job_code: str):
    df_career = pd.read_sql('career_info', engine)
    df_career.to_csv(f'./raw_data/df_career.csv', index=False)
    print('career data frame saved.')

    jobs_list = df_career['normalized_job_code'].unique()
    jobs_raw_uni = pd.DataFrame(jobs_list)
    jobs_raw_uniques = jobs_raw_uni.rename(columns={0: 'normalized_job_code'})

    jobs_raw_uniques['job_name'] = jobs_raw_uniques.iloc[1:, 0].apply(api_caller)
    job_name_df = pd.DataFrame(jobs_raw_uniques)
    job_name_df.to_csv('./raw_data/jobs_dataframe.csv')
    print('We have all unique jobs.')

def country_names():
    print('Retrieving countries lists...')

    url = 'https://www.iban.com/country-codes'
    raw_codes = pd.read_html(url, header=0)[0]

    # only keeping columns that are needed
    country_codes = raw_codes.drop(columns=['Alpha-3 code', 'Numeric'])
    country_code = country_codes.rename(columns={'Country': 'country', 'Alpha-2 code': 'country_code'})

    country_code.to_csv('./raw_data/country_codes_scrapped.csv', index=False)
    print('Official country names scrapped.')





