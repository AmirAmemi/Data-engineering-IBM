from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 


def extract(url, table_attribs):
    ''' The purpose of this function is to extract the required
    information from the website and save it to a dataframe. The
    function returns the dataframe for further processing. '''

    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        
        if len(col)!=0 :
            if col[1].find('a') is not None :
                # print(col[1].text.strip())
                data_dict = {"Name": col[1].find_all('a')[1]['title'],
                            "MC_USD_Billio": float(col[2].contents[0]),}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df,df1], ignore_index=True)

    print(df)
    return df

def transform(df):
    ''' This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe.'''

    exchange = pd.read_csv('C:/Users/amira/Desktop/Data engineering IBM/ETL Final project/exchange_rate.csv')
    exchange = exchange.set_index('Currency').to_dict()['Rate']
    USD_list = df["MC_USD_Billio"].tolist()
    # USD_list1 = [float("".join(x.split(','))) for x in USD_list]
    GBP_list = [np.round(x*exchange['GBP'],2) for x in USD_list]
    EUR_list = [np.round(x*exchange['EUR'],2) for x in USD_list]
    INR_list = [np.round(x*exchange['INR'],2) for x in USD_list]

    df["MC_GBP_Billion"] = GBP_list
    df["MC_EUR_Billion"] = EUR_list
    df["MC_INR_Billion"] = INR_list
    
    return df


def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''

    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe to as a database table
    with the provided name. Function returns nothing.'''

    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''

    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the 
    code execution to a log file. Function returns nothing.'''

    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("C:/Users/amira/Desktop/Data engineering IBM/ETL Final project/code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')  


url = 'https://web.archive.org/web/20230908091635%20/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name", "MC_USD_Billio"]
db_name = 'C:/Users/amira/Desktop/Data engineering IBM/ETL Final project/Banks.db'
table_name = 'Largest_banks'
csv_path = 'C:/Users/amira/Desktop/Data engineering IBM/ETL Final project/Largest_banks_data.csv'

log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df)

log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, csv_path)

log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect('World_Economies.db')

log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as table. Running the query')

query_statement = f"SELECT * FROM Largest_banks"
run_query(query_statement, sql_connection)

query_statement = f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_query(query_statement, sql_connection)

query_statement = f"SELECT Name from Largest_banks LIMIT 5"
run_query(query_statement, sql_connection)

log_progress('Process Complete.')

sql_connection.close()
log_progress('server connection closed.')



 