import sqlite3

# Connect to the SQLite database
conn = sqlite3.connect('stock_data.db')
cursor = conn.cursor()

# Print metadata
print("Metadata:")
cursor.execute('SELECT * FROM metadata')
metadata_results = cursor.fetchall()
for row in metadata_results:
    print(row)

print("\nStock Data:")
# Print stock data
cursor.execute('SELECT * FROM stock_data')
stock_data_results = cursor.fetchall()
for row in stock_data_results:
    print(row)

# Close the connection
conn.close()
