import pandas as pd
import requests
from sqlalchemy import create_engine
from bs4 import BeautifulSoup
import regex as re
import numpy as np
import modin.pandas as mp

# analysis functions

def analyze_ch_1():
    full_set = pd.read_csv('./raw_data/full_set.csv')

    challenge_1_df = full_set[['job_name', 'country', 'gender', 'uuid']]
    challenge_1df = challenge_1_df.groupby(['country', 'job_name', 'gender', ])['uuid'].count().reset_index()
    challenge_1df.rename(columns={'country': 'Country', 'job_name': 'Job Title', 'gender': 'Gender', 'uuid': 'Quantity'}, inplace=True)
    challenge_1df['Percentage %'] = challenge_1df['Quantity'] / challenge_1df['Quantity'].sum() * 100
    challenge_1df.to_csv('./final_data/challenge_1.csv', index=False)

