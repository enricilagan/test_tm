import pyodbc
import pandas as pd
from answer_2_config import *

server = server_config
database = database_config
username = username_config
password = password_config   
driver='{ODBC Driver 17 for SQL Server}'

## This iteration currently uses truncate load logic for loading data into Azure SQL DB

def load_to_db():
    file_loc = 'https://'+storage_acct+'.blob.core.windows.net/'+azure_container+'/'
    df = pd.read_csv(file_loc + 'cleaned_dailycheckin.csv')

    # Setup driver and database details
    cnxn = pyodbc.connect('DRIVER={0};SERVER={1};DATABASE={2};UID={3};PWD={4}'.format(driver, server, database, username, password))

    # check current content of database (if there are data in DB it will be removed)
    with cnxn:
        with cnxn.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM daily_checkin")
            row = cursor.fetchone()
            check_data = row[0]
    

    print('... Checking if there are data in [daily_checkin] Database')
    if check_data > 1:
        print('... DB contains [{0}] records'.format(check_data))
    else:
        print('... DB does not contain data proceeding with insert.')

    # Run delete statement on Database
    with cnxn:
        with cnxn.cursor() as cursor:
            if check_data > 0:
                print('... Deleting data in [daily_checkin] database')
                cursor.execute("DELETE from daily_checkin")  
        cnxn.commit()
        

    insert_statement = 'INSERT INTO daily_checkin (users, time_stamp, hours_rendered, project, logged_time) values(?,?,?,?,?)'

    # Insert data from cleaned csv file into DB.
    with cnxn:
        with cnxn.cursor() as cursor:
            print('... Inserting data from CSV to [daily_checkin] DB.')
            for index, row in df.iterrows():
                cursor.execute(insert_statement, row.user, row.timestamp, row.hours, row.project, row.logged_time)
                if index % 500 == 0:
                    print('... Inserted {0} rows into database'.format(index))
            print('... Insert Completed.')
        cnxn.commit()

    with cnxn:
        with cnxn.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM daily_checkin")
            row = cursor.fetchone()
            check_data = row[0]
            print('... [{0}] records inserted.'.format(check_data))

    return None

if __name__ == '__main__':
    load_to_db()
