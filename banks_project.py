

# Define file paths and table attributes
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
csv_path = 'exchange_rate.csv'  # Correct path to exchange rate CSV
db_name = 'Banks.db'
table_name = 'Largest_banks'
log_file = 'process_log.txt'
table_attribs = ["Name", "MC_USD_Billion"]  # Ensure this is defined

# Import exchange rate data
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 
exchange_rate = pd.read_csv("https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv")

def log_progress(message):
    '''This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing.'''
    timestamp_format = "%Y-%m-%d %H:%M:%S"  # Corrected timestamp format
    now = datetime.now()  # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + " : " + message + "\n")

def extract(url, table_attribs):
    '''This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing.'''
    page = requests.get(url).text
    soup = BeautifulSoup(page, "html.parser")

    df = pd.DataFrame(columns=table_attribs)
    tables = soup.find_all("tbody")
    rows = tables[0].find_all("tr")

    for row in rows:
        col = row.find_all("td")
        if len(col) != 0:
            data_dict = {"Name": col[1].find_all("a")[1]["title"],
                         "MC_USD_Billion": float(col[2].contents[0][:-1])}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)

    return df

def transform(df, csv_path):
    '''This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies.'''
    # Read exchange rate CSV file
    exchange_rate = pd.read_csv("https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv")
    
    # Convert to a dictionary with "Currency" as keys and "Rate" as values
    exchange_rate = exchange_rate.set_index("Currency").to_dict()["Rate"]

    # Add MC_GBP_Billion, MC_EUR_Billion, and MC_INR_Billion
    # columns to dataframe. Round off to two decimals
    df["MC_GBP_Billion"] = [np.round(x * exchange_rate["GBP"], 2) for x in df["MC_USD_Billion"]]
    df["MC_EUR_Billion"] = [np.round(x * exchange_rate["EUR"], 2) for x in df["MC_USD_Billion"]]
    df["MC_INR_Billion"] = [np.round(x * exchange_rate["INR"], 2) for x in df["MC_USD_Billion"]]
    
    return df

def load_to_csv(df, output_path):
    '''This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path, index=False)

def load_to_db(df, sql_connection, table_name):
    '''This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    '''This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing.'''
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

# Main ETL process
log_progress("Data extraction initiated")
df = extract(url, table_attribs)
print(df)

log_progress("Data extraction complete. Initiating Transformation process")
df = transform(df, csv_path)
print(df)

log_progress("Data transformation complete. Initiating Loading process")
# Create a database connection
sql_connection = sqlite3.connect(db_name)

# Load data to CSV and DB
load_to_csv(df, 'Largest_banks_data.csv')
load_to_db(df, sql_connection, table_name)
log_progress("Data saved to CSV file and loaded to Database as a table, Executing queries")

# Execute queries
query_statement = f"SELECT * FROM {table_name}"
run_query(query_statement, sql_connection)

query_statement = f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
run_query(query_statement, sql_connection)

query_statement = f"SELECT Name FROM {table_name} LIMIT 5"
run_query(query_statement, sql_connection)

log_progress("Process Complete")

# Close database connection
sql_connection.close()
log_progress("Server Connection closed")
