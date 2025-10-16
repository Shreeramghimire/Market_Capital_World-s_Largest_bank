# Code for ETL operations on Largest Banks data

# Importing the required libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name", "MC_USD_Billion"]
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = './Largest_banks_data.csv'
exchange_rate_csv = './exchange_rate.csv'

def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

def extract(url, table_attribs):
    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    
    tables = data.find_all('table', {'class': 'wikitable'})
    
    if tables:
        target_table = tables[0]
    else:
        tables = data.find_all('table')
        target_table = tables[0] if tables else None
    
    if target_table:
        rows = target_table.find_all('tr')
        count = 0
        
        for row in rows:
            if count >= 10: 
                break
                
            col = row.find_all('td')
            if len(col) >= 3:
                name_cell = col[0] if col[0].find('a') else col[1]
                name = name_cell.get_text(strip=True)
                
                mc_cell = col[2] if len(col) > 2 else col[1]
                mc_text = mc_cell.get_text()
                
                mc_clean = mc_text.replace('US$', '').replace(',', '').split('[')[0].strip()
                
                if mc_clean and mc_clean[-1] in ['\n', ' ', '\t']:
                    mc_clean = mc_clean[:-1]
                
                try:
                    market_cap = float(mc_clean)
                    data_dict = {"Name": name, "MC_USD_Billion": market_cap}
                    df1 = pd.DataFrame(data_dict, index=[0])
                    df = pd.concat([df,df1], ignore_index=True)
                    count += 1
                except ValueError as e:
                    print(f"Error converting market cap value: '{mc_clean}' - {e}")
                    continue
    
    return df

def transform(df, csv_path):
    log_progress("Reading exchange rate CSV file")
    exchange_rates_df = pd.read_csv(csv_path)
    
    print("Exchange Rate Data:")
    print(exchange_rates_df)
    
    exchange_rate = exchange_rates_df.set_index('Currency').to_dict()['Rate']
    
    print("Exchange Rates Dictionary:")
    print(exchange_rate)
    
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]
    
    log_progress("Data transformation with exchange rates complete")
    return df

def load_to_csv(df, output_path):
    df.to_csv(output_path, index=False)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    print(f"Query: {query_statement}")
    query_output = pd.read_sql(query_statement, sql_connection)
    print("Output:")
    print(query_output)
    print("\n")

def run_queries(query_statements, sql_connection):
    log_progress("Running queries on the database")
    
    for i, query in enumerate(query_statements, 1):
        print(f"Query {i}:")
        run_query(query, sql_connection)
        log_progress(f"Executed Query {i}")

with open("./code_log.txt", "w") as f:
    f.write("ETL Process Log - Largest Banks Data\n")
    f.write("=" * 50 + "\n")

query_statements = [
    "SELECT * FROM Largest_banks",
    "SELECT AVG(MC_GBP_Billion) AS Average_MC_GBP_Billion FROM Largest_banks",
    "SELECT Name FROM Largest_banks LIMIT 5"
]

log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)
print("Extracted data:")
print(df)
print(f"Data types after extraction:\n{df.dtypes}")
log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df, exchange_rate_csv)
print("Transformed data:")
print(df)
log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, csv_path)
log_progress('Data saved to CSV file')

log_progress('Initiating SQLite3 database connection')
sql_connection = sqlite3.connect(db_name)
log_progress('SQL Connection initiated successfully')

load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as table')

run_queries(query_statements, sql_connection)

log_progress('Process Complete.')

sql_connection.close()
log_progress('Database connection closed')