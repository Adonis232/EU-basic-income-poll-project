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


#query to get the df information

df_personal = pd.read_sql('personal_info',engine)
df_country = pd.read_sql('country_info',engine)
df_career = pd.read_sql('career_info',engine)
df_poll = pd.read_sql('poll_info',engine)

engine.dispose()


def gender_modifier(x):
    if x == 'FeMale':
        return 'Female'
    elif x == 'Fem':
        return 'Female'
    elif x == 'female':
        return 'Female'
    elif x == 'male':
        return 'Male'
    elif x == 'Male':
        return 'Male'
    
df_personal['dem_has_children'].unique()


def children_modifier(x):
    if x == 'NO':
        return 'no'
    elif x == 'nO':
        return 'no'
    elif x == 'No':
        return 'no'
    elif x == 'YES':
        return 'yes'
    elif x == 'yES':
        return 'yes'

#appliying my definition to clean gender and dem_has_children columns
df_personal['gender_modified'] = df_personal['gender'].apply(gender_modifier)
df_personal['dem_has_children_mo'] = df_personal['dem_has_children'].apply(children_modifier)

#from df raw, dropping the old and unclean 'gender' and dem_has_children columns
df_personal_clean = df_personal.drop(columns=['gender', 'dem_has_children'])


#rename column
df_personal_clean = df_personal_clean.rename(columns={'gender_modified':'gender','dem_has_children_mo':'dem_has_children'})

df_personal_clean.isnull().sum()#no nulls
df_personal_clean.head()
df_personal_clean['age_group'].unique()

df_personal_clean.to_csv('../raw_data/df_personal.csv',index=False)
#country info dataframe

df_country.isnull().sum()# no nulls
df_country.to_csv('../raw_data/df_country.csv',index=False)#sent them to raw data folder

#career info dataframe
df_career['dem_education_level'].unique()

df_career['normalized_job_code'].fillna('None',inplace=True)
df_career['dem_education_level'].fillna('None',inplace=True)

df_career.to_csv('../raw_data/df_career.csv',index=False)

#poll info dataframe
df_poll.isnull().sum()

df_poll.to_csv('../raw_data/df_poll.csv',index=False)

jobs_list = df_career['normalized_job_code'].unique()
jobs_raw_uni = pd.DataFrame(jobs_list)
jobs_raw_uniques = jobs_raw_uni.rename(columns={0:'normalized_job_code'})

#df_career
jobs_raw_uniques

def api_caller(x):
    req=[]
    r = requests.get('http://api.dataatwork.org/v1/jobs/' + str(x))
    req = r.json()
    return req['normalized_job_title']


#df_career['new_job_code'] = df_career['normalized_job_code'].apply(api_caller)
#api_caller('ac47656fd51c2cd8037057262c910dc4')

jobs_raw_uniques['job_name'] = jobs_raw_uniques.iloc[1:,0].apply(api_caller)

jobs_raw_uniques 

job_name_df = pd.DataFrame(jobs_raw_uniques)


job_name_df.to_csv('../raw_data/jobs_dataframe.csv')

#country codes

country_list = df_country['country_code'].unique().tolist()
country_list



url = 'https://www.iban.com/country-codes' 
raw_codes = pd.read_html(url,header=0)[0]
raw_codes

#only keeping columns that are needed
country_codes = raw_codes.drop(columns=['Alpha-3 code','Numeric'])
country_code = country_codes.rename(columns={'Country':'country','Alpha-2 code':'country_code'})
country_code

country_code.to_csv('../raw_data/country_codes.csv',index=False)

#tryin to get it from another web 

#country codes through webscrapping

""""url = 'https://ec.europa.eu/eurostat/statistics-explained/index.php/Glossary:Country_codes' 
html = requests.get(url).content
soup = BeautifulSoup(html, 'lxml')
#parsing and getting the table
countries = soup.find_all('td')

country_raw_lst = []
for item in countries:
    if str(item) != '':
        country_raw_lst.append(str(item))

country_raw_lst[0::1]"""

#Merging countries and country codes
countries_merged = pd.merge(country_code,df_country,how='right',on='country_code')

countries_merged .to_csv('../raw_data/country_info_merged.csv',index=False)

full_personal = pd.merge(df_personal,countries_merged,how='left',on='uuid')


merged_interview = pd.merge(full_personal,df_poll,how='left',on='uuid')

full_poll = pd.merge(merged_interview,df_career,how='left',on='uuid')

final_df = pd.merge(full_poll,job_name_df,how='left',on='normalized_job_code') 
#final_df.to_csv('../raw_data/full_dataframe.csv',index=False) 
df_modified = final_df.drop(columns=['gender','dem_has_children']) 
df_modified


full_set = df_modified.rename(columns= {'gender_modified':'gender','dem_has_children_mo':'dem_has_children'})

full_set.to_csv('../raw_data/full_set.csv',index=False)


#