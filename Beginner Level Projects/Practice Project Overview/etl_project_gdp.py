from bs4 import BeautifulSoup
import requests
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime

# URL and file/database parameters
url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_attribs = ["Country", "GDP_USD_millions"]
db_name = 'World_Economies.db'
table_name = 'Countries_by_GDP'
csv_path = './Countries_by_GDP.csv'
log_file = './etl_project_log.txt'

def extract(url, table_attribs):
    """Extract GDP data table from Wikipedia page"""
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)

    tables = soup.find_all('tbody')
    rows = tables[2].find_all('tr')

    for row in rows:
        cols = row.find_all('td')
        if len(cols) != 0 and cols[0].find('a') is not None and '—' not in cols[2]:
            country = cols[0].a.text.strip()
            gdp = cols[2].text.strip()
            data_dict = {"Country": country, "GDP_USD_millions": gdp}
            df = pd.concat([df, pd.DataFrame([data_dict])], ignore_index=True)
    return df

def transform(df):
    """Convert GDP from millions to billions, round to 2 decimals"""
    df["GDP_USD_millions"] = df["GDP_USD_millions"].str.replace(',', '')
    df["GDP_USD_billions"] = (df["GDP_USD_millions"].astype(float) / 1000).round(2)
    df.drop(columns=["GDP_USD_millions"], inplace=True)
    return df

def load_to_csv(df, csv_path):
    df.to_csv(csv_path, index=False)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    print(f"\nRunning query: {query_statement}")
    result = pd.read_sql(query_statement, sql_connection)
    print(result)

def log_progress(message):
    timestamp_format = '%A, %d %B %Y %I:%M:%S %p'  # e.g., Sunday, 06 July 2025 03:24:24 PM
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, 'a') as f:
        f.write(f"{timestamp} - {message}\n")

# ---- ETL Process ----

log_progress('Preliminaries complete. Initiating ETL process.')

df = extract(url, table_attribs)
log_progress('Data extraction complete. Initiating transformation process.')

df = transform(df)
log_progress('Data transformation complete. Initiating loading process.')

load_to_csv(df, csv_path)
log_progress('Data saved to CSV file.')

with sqlite3.connect(db_name) as conn:
    log_progress('SQL connection initiated.')
    load_to_db(df, conn, table_name)
    log_progress('Data loaded to database table. Running query.')
    query = f"SELECT * FROM {table_name} WHERE GDP_USD_billions >= 100"
    run_query(query, conn)

log_progress('ETL process complete.')
print('\n✅ ETL process finished successfully.')
