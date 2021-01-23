import pandas as pd
import numpy as np
import os
from datetime import datetime as dt 
import locale
from autocorrect import Speller
from azure.storage.blob import BlobServiceClient
import answer_2_config as cfg

locale.setlocale(locale.LC_TIME, 'ru_RU.UTF-8')

# Current script
file_loc = 'https://'+cfg.storage_acct+'.blob.core.windows.net/'+azure_container+'/'

df = pd.read_csv(file_loc + 'dailycheckins.csv')

#print(df.loc[df['user'].isnull()])

# Used to check if the column is a time format
def TimeFormat(t):
    try:
        dt.strptime(t, '%d %B %Y  %H:%M')
        return True
    except ValueError:
        return False

# Clean Data
def clean_df(df):
    # Set Time to Russian Local Time
    locale.setlocale(locale.LC_TIME, 'ru_RU.UTF-8')
    spell = Speller(lang='en')

    # place something here to clean data
    df = df.dropna()
    df.loc[:,'user'] = df['user'].str.capitalize()

    #### Cleaning timestamp column ####
    # Clean for format with Russian Month Name
    df.loc[:,'lformat'] = [TimeFormat(x) for x in df['timestamp']]
    df.loc[df['lformat']==True, 'timestamp'] = [dt.strptime(x, '%d %B %Y  %H:%M') for x in df.loc[df['lformat']==True, 'timestamp']]
    # Create UTC_TS to make code more readable
    df.loc[df['timestamp'].str.endswith('UTC')==True,'timestamp'] = [dt.strptime(x[:16], '%Y-%m-%d %H:%M') for x in df.loc[df['timestamp'].str.endswith('UTC')==True,'timestamp']]

    df.loc[:,'isdt'] = [isinstance(x, dt) for x in df['timestamp']]

    # Clean remaining format of date
    df.loc[df['isdt']==False,'timestamp'] = pd.to_datetime(df.loc[df['isdt']==False,'timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')

    df.loc[:,'isdt'] = [isinstance(x, dt) for x in df['timestamp']]

    #### Cleaning timestamp column ####
    # Correct Culture and Management
    others = ['misc','qgis','d3js','airflow','random','internal','internals','new-office','laptop','breakout']

    df.loc[df['project'].str.contains('andman'),'project'] = [spell(x.replace('and',' and ')) for x in df.loc[df['project'].str.contains('andman'),'project']]
    df.loc[np.logical_and(df['project'].str.startswith('op'), df['project'].str.endswith('min')),'project'] = 'ops and admin'
    df.loc[df['project'].str.startswith('blog'),'project'] = 'blog ideas'
    df.loc[df['project'].str.startswith('machine'),'project'] = 'machine learning'
    df.loc[df['project'].str.startswith('data'),'project'] = 'data storytelling'
    df.loc[df['project'].str.startswith('traffic'),'project'] = 'transit'
    df.loc[df['project'].str.startswith('project'),'project'] = 'projects'
    df.loc[df['project'].str.endswith('workshop'),'project'] = 'workshops'
    df.loc[df['project'].isin(['pandas','braintooling','overthinking','brief','sqool','weeklygoals']),'project'] = 'learning'
    df.loc[df['project'].isin(['branding','legal','marketing','media','events','dogood']),'project'] = 'events'
    df.loc[df['project'].isin(['design','debugging','engineering','strategy','paperwork']),'project'] = 'engineering'
    df.loc[df['project'].str.startswith('hir'),'project'] = 'recruitment'
    df.loc[df['project'].isin(others),'project'] = 'miscellaneous'
    df.loc[df['project'].isin(['leave','lunch','pm','friendship','fatnesscheckin']),'project'] = 'personal time'

    ## Create new column for detailed logged time
    df.loc[:,'minutes'] = [(x % 1)*60 for x in df['hours']]
    df.loc[:,'seconds'] = [int((x % 1)*60) for x in df['minutes']]
    df.loc[:,'hours_int'] = [int(x) for x in df['hours']]
    df.loc[:,'minutes'] = [int(x) for x in df['minutes']]
    df.loc[:,'logged_time'] = df[['hours_int', 'minutes','seconds']].apply(lambda x: ':'.join(x.dropna().astype(str).str.zfill(2)),axis=1)

    return df.loc[:,['user','timestamp','hours','project','logged_time']]


if __name__ == '__main__':
    cleaned = clean_df(df)

    cleaned.to_csv('../cleaned_dailycheckin.csv', index=False)

    blob_service_client = BlobServiceClient.from_connection_string(cfg.conn_string)

    blob_client = blob_service_client.get_blob_client(cfg.azure_container, blob='cleaned_dailycheckin.csv')
    upload_file_path = '../cleaned_dailycheckin.csv'
    with open(upload_file_path, "rb  ") as data:
        blob_client.upload_blob(data)
