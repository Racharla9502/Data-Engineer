import sqlite3
import pandas as pd

# Define database and table
db_file = 'STAFF.db'
table_name = 'INSTRUCTOR'
attribute_list = ['ID', 'FNAME', 'LNAME', 'CITY', 'CCODE']

# Read CSV data
file_path = r'/Users/vyshnav/Downloads/Projects/Data Engineer/Beginner Level Projects/Project_03: Accessing Databases using Python Script/INSTRUCTOR.csv'
df = pd.read_csv(file_path, names=attribute_list)


with sqlite3.connect(db_file) as conn:
    # Load the CSV into the database
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    print('✅ Table created and data loaded successfully.')

    # Query 1: Display all rows
    query = f"SELECT * FROM {table_name}"
    print(f"\n{query}")
    print(pd.read_sql(query, conn))

    # Query 2: Display only the FNAME column
    query = f"SELECT FNAME FROM {table_name}"
    print(f"\n{query}")
    print(pd.read_sql(query, conn))

    # Query 3: Display total row count
    query = f"SELECT COUNT(*) as total_rows FROM {table_name}"
    print(f"\n{query}")
    print(pd.read_sql(query, conn))

    # Data to append
    data_append = pd.DataFrame({
        'ID': [100],
        'FNAME': ['John'],
        'LNAME': ['Doe'],
        'CITY': ['Paris'],
        'CCODE': ['FR']
    })

    # Append new data
    data_append.to_sql(table_name, conn, if_exists='append', index=False)
    print('\n✅ New row appended successfully.')

    # Query 4: New total row count
    query = f"SELECT COUNT(*) as total_rows FROM {table_name}"
    print(f"\n{query}")
    print(pd.read_sql(query, conn))