import sqlite3
from datetime import datetime, timedelta
import random


# Function to generate a random timestamp
def generate_timestamp(primary_key):
    start_date = datetime(2023, 6, 1)
    random_date = start_date + timedelta(days=primary_key)
    random_time = timedelta(hours=random.randint(0, 23), minutes=random.randint(0, 59), seconds=random.randint(0, 59))
    timestamp = random_date + random_time
    return timestamp.strftime("%Y-%m-%d %H:%M:%S")


database_path = 'C:/Users/30698/Desktop/DWS/2nd_semester/Distributed_Data_Processing/projects/programming/' + \
                'python_project/databases/'

# Connect to the first SQLite database file
conn1 = sqlite3.connect(database_path+'database1.db')
cursor1 = conn1.cursor()

# Delete the table if it exists
cursor1.execute('DROP TABLE IF EXISTS table1')

# Create a table in the first database if it doesn't exist
cursor1.execute('''CREATE TABLE table1
                  (id INTEGER PRIMARY KEY AUTOINCREMENT,
                   name TEXT,
                   email TEXT,
                   timestamp TEXT)''')

# Insert random data into the table with random timestamps
for i in range(1, 20):
    name = f"Name_{random.randint(1, 300)}"
    email = f"email_{random.randint(1, 300)}@example.com"
    timestamp = generate_timestamp(i)
    cursor1.execute('INSERT INTO table1 (name, email, timestamp) VALUES (?, ?, ?)', (name, email, timestamp))


# Commit the changes to the first database
conn1.commit()
conn1.close()

# Connect to the second SQLite database file
conn2 = sqlite3.connect(database_path+'database2.db')
cursor2 = conn2.cursor()

# Delete the table if it exists
cursor2.execute('DROP TABLE IF EXISTS table2')

# Create a table in the second database if it doesn't exist
cursor2.execute('''CREATE TABLE table2
                  (id INTEGER PRIMARY KEY AUTOINCREMENT,
                   name TEXT,
                   email TEXT,
                   timestamp TEXT)''')

# Insert random data into the table with random timestamps
for i in range(1, 50):
    name = f"Name_{random.randint(1, 300)}"
    email = f"email_{random.randint(1, 300)}@example.com"
    timestamp = generate_timestamp(i)
    cursor2.execute('INSERT INTO table2 (name, email, timestamp) VALUES (?, ?, ?)', (name, email, timestamp))

# Commit the changes to the second database
conn2.commit()
conn2.close()
