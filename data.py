import requests
import json
import sqlite3

# Replace this list with your actual stock symbols
symbols = ['ORCL', 'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'FB', 'TSLA', 'NFLX', 'NVDA', 'CSCO', 
           'INTC', 'IBM', 'ADBE', 'PYPL', 'CRM', 'NFLX', 'DIS', 'PFE', 'VZ', 'T', 
           'CMCSA', 'V', 'MA', 'WMT', 'PEP', 'KO', 'ABT', 'NKE', 'MRK', 'XOM', 
           'CVX', 'CSX', 'MDLZ', 'AMGN', 'GILD', 'AVGO', 'TXN', 'QCOM', 'INTU', 
           'LMT', 'BA', 'CAT', 'GE', 'UNH', 'JNJ', 'TMO', 'MS', 'GS', 'JPM', 
           'C', 'BAC', 'WFC', 'USB', 'DHR', 'NEE', 'SO', 'SRE', 'EXC', 'ED', 
           'AEP', 'PNW', 'PPL', 'CMS', 'XEL', 'AWK', 'DTE', 'ES', 'ETR', 'FE', 
           'MPC', 'VLO', 'OXY', 'PSX', 'KMI', 'ET', 'SLB', 'WMB', 'VTRS', 'CHTR']

# Connect to SQLite database (or create it if it doesn't exist)
conn = sqlite3.connect('stock_data.db')
cursor = conn.cursor()

# Drop the existing stock_data table if it exists
cursor.execute('DROP TABLE IF EXISTS stock_data')
# Create the tables if they don't exist
cursor.execute('''
    CREATE TABLE IF NOT EXISTS stock_data (
        date TEXT,
        symbol TEXT,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume INTEGER,
        PRIMARY KEY (date, symbol)
    )
''')
cursor.execute('DROP TABLE IF EXISTS metadata')  # Drop existing metadata table
cursor.execute('''
    CREATE TABLE IF NOT EXISTS metadata (
        symbol TEXT PRIMARY KEY,
        last_refreshed TEXT,
        timezone TEXT
    )
''')

# Function to fetch and store stock data for a symbol
def fetch_and_store_data(symbol):
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey=2JEF8V3LMIPHY7YK'
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()  # Parse the JSON response

        # Save the metadata
        metadata = data.get("Meta Data", {})
        cursor.execute('''
            INSERT OR REPLACE INTO metadata (symbol, last_refreshed, timezone)
            VALUES (?, ?, ?)
        ''', (
            symbol,
            metadata.get("3. Last Refreshed"),
            metadata.get("5. Time Zone")
        ))

        # Extract time series data
        time_series = data.get("Time Series (Daily)", {})

        # Insert each day's data into the database
        for date, daily_data in time_series.items():
            cursor.execute('''
                INSERT OR REPLACE INTO stock_data (date, symbol, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                date,
                symbol,
                float(daily_data["1. open"]),
                float(daily_data["2. high"]),
                float(daily_data["3. low"]),
                float(daily_data["4. close"]),
                int(daily_data["5. volume"])
            ))

        print(f"Data for {symbol} has been saved.")
    else:
        print(f"Failed to fetch data for {symbol}. Status code: {response.status_code}")

# Fetch and store data for each symbol
for symbol in symbols:
    fetch_and_store_data(symbol)

# Commit the transaction and close the connection
conn.commit()
conn.close()
print("All data has been saved to the SQLite database (stock_data.db).")
