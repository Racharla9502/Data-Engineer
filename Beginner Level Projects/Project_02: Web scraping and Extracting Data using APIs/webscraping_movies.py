import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3

# Step 1: Fetch the page
url = "https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films"
response = requests.get(url)
response.raise_for_status()  # will raise an error if the request failed

# Step 2: Parse HTML
soup = BeautifulSoup(response.text, 'html.parser')

# Step 3: Find the table containing the data
# Looking for table with the top 100 films
table = soup.find('table', {'class': 'wikitable'})

# Check if table found
if table is None:
    raise Exception("Could not find the data table on the page.")

# Step 4: Extract rows
rows = table.find_all('tr')[1:]  # skip header row

data = []
for row in rows[:50]:  # top 50
    cols = row.find_all(['td', 'th'])
    if len(cols) >= 4:
        avg_rank = cols[0].text.strip()
        film = cols[1].text.strip()
        year = cols[2].text.strip()
        data.append({
            'Average Rank': avg_rank,
            'Film': film,
            'Year': year
        })

# Step 5: Convert to DataFrame
df = pd.DataFrame(data)

# Step 6: Save to CSV
df.to_csv('top_50_films.csv', index=False)

# Step 7: Save to SQLite database
conn = sqlite3.connect('Movies.db')
df.to_sql('Top_50', conn, if_exists='replace', index=False)
conn.close()

print("✅ Extraction complete: Saved to top_50_films.csv and Movies.db")
