import sqlite3




database_path = 'C:/Users/30698/Desktop/DWS/2nd_semester/Distributed_Data_Processing/projects/programming/' + \
                'python_project/databases/'

# Connect to the first SQLite database file
conn1 = sqlite3.connect(database_path+'database1.db')
cursor1 = conn1.cursor()

# Execute a query to fetch all rows from the first table
cursor1.execute('SELECT * FROM table1')
rows1 = cursor1.fetchall()

# Check if the first table is filled
if len(rows1) > 0:
    print("Table 1 is filled.")
    # Print the retrieved data
    for row in rows1:
        print(row)
else:
    print("Table 1 is empty.")

# Close the connection to the first database
conn1.close()

# Connect to the second SQLite database file
conn2 = sqlite3.connect(database_path+'database2.db')
cursor2 = conn2.cursor()

# Execute a query to fetch all rows from the second table
cursor2.execute('SELECT * FROM table2')
rows2 = cursor2.fetchall()

# Check if the second table is filled
if len(rows2) > 0:
    print("Table 2 is filled.")
    # Print the retrieved data
    for row in rows2:
        print(row)
else:
    print("Table 2 is empty.")

# Close the connection to the second database
conn2.close()
