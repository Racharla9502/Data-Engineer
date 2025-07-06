import pandas as pd
import requests
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime
from io import StringIO

# Project parameters
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
exchange_rate_csv = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = './Largest_banks_data.csv'
log_file = 'code_log.txt'

# Task 1: log_progress()
def log_progress(message):
    timestamp = datetime.now().strftime('%A, %d %B %Y %I:%M:%S %p')
    with open(log_file, 'a') as f:
        f.write(f"{timestamp} - {message}\n\n")  # Add a blank line after each entry

# Task 2: extract()
def extract():
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')
    tables = soup.find_all('table', {'class': 'wikitable'})

    # Use StringIO to avoid FutureWarning
    df = pd.read_html(StringIO(str(tables)))[0]

    print("\n📌 Available columns:", df.columns)

    # Dynamically find correct columns
    name_col = next((col for col in df.columns if 'Name' in col or 'Bank' in col), None)
    mc_col = next((col for col in df.columns if 'Market cap' in col or 'US$ billion' in col), None)

    if not name_col or not mc_col:
        raise ValueError(f"❌ Could not find expected columns. Found: {df.columns}")

    df = df[[name_col, mc_col]]
    df.columns = ['Name', 'MC_USD_Billion']

    # Keep only top 10
    df = df.head(10)

    # Convert market cap to numeric
    df['MC_USD_Billion'] = pd.to_numeric(df['MC_USD_Billion'], errors='coerce')

    print("\n✅ Extracted data:")
    print(df)

    return df

# Task 3: transform()
def transform(df):
    exchange_rates = pd.read_csv(exchange_rate_csv)
    rates = dict(zip(exchange_rates['Currency'], exchange_rates['Rate']))

    df['MC_GBP_Billion'] = (df['MC_USD_Billion'] * rates['GBP']).round(2)
    df['MC_EUR_Billion'] = (df['MC_USD_Billion'] * rates['EUR']).round(2)
    df['MC_INR_Billion'] = (df['MC_USD_Billion'] * rates['INR']).round(2)

    print("\n✅ Transformed data:")
    print(df)

    return df

# Task 4: load_to_csv()
def load_to_csv(df, path):
    df.to_csv(path, index=False)

# Task 5: load_to_db()
def load_to_db(df, db_name, table_name):
    with sqlite3.connect(db_name) as conn:
        df.to_sql(table_name, conn, if_exists='replace', index=False)

# Task 6: run_queries()
def run_queries(db_name, table_name):
    with sqlite3.connect(db_name) as conn:
        query1 = f"SELECT * FROM {table_name}"
        print("\n📊 All data:")
        print(pd.read_sql(query1, conn))

        query2 = f"SELECT AVG(MC_GBP_Billion) as Avg_GBP FROM {table_name}"
        print("\n📊 Average market cap in GBP:")
        print(pd.read_sql(query2, conn))

        query3 = f"SELECT Name, MAX(MC_INR_Billion) as Max_INR FROM {table_name}"
        print("\n📊 Bank with highest market cap in INR:")
        print(pd.read_sql(query3, conn))

# ---- 🚀 ETL Pipeline ----

log_progress('ETL process started.')

log_progress('Extracting data from web.')
df = extract()
log_progress('Extraction complete.')

log_progress('Transforming data using exchange rates.')
df = transform(df)
log_progress('Transformation complete.')

log_progress('Loading data to CSV.')
load_to_csv(df, csv_path)
log_progress('Data saved to CSV.')

log_progress('Loading data to database.')
load_to_db(df, db_name, table_name)
log_progress('Data saved to database.')

log_progress('Running SQL queries.')
run_queries(db_name, table_name)
log_progress('Queries executed.')

log_progress('ETL process completed successfully.')
print('\n✅ ETL process finished! Check CSV, database, and log file.')
