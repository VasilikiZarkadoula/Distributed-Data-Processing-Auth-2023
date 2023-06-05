from datetime import datetime, timedelta
import random


class CreateTable:
    def __init__(self, conn1, conn2):
        """
        Initialize the HashJoin object.

        :param conn1: Connection to the first database
        :param conn2: Connection to the second database
        """
        self.conn1 = conn1
        self.conn2 = conn2
        self.cursor1 = conn1.cursor()
        self.cursor2 = conn2.cursor()

    @staticmethod
    def generate_timestamp(primary_key):
        start_date = datetime(2023, 6, 1)
        random_date = start_date + timedelta(days=primary_key)
        random_time = timedelta(hours=random.randint(0, 23), minutes=random.randint(0, 59), seconds=random.randint(0, 59))
        timestamp = random_date + random_time
        return timestamp.strftime("%Y-%m-%d %H:%M:%S")

    def create_table(self, connection, cursor, table_name, num_records):

        # Delete the table if it exists
        cursor.execute(f'DROP TABLE IF EXISTS {table_name}')

        # Create a table if it doesn't exist
        cursor.execute(f'''CREATE TABLE {table_name}
                          (id INTEGER PRIMARY KEY AUTOINCREMENT,
                           name TEXT,
                           email TEXT,
                           timestamp TEXT)''')

        # Insert random data into the table with random timestamps
        for i in range(1, num_records):
            name = f"Name_{random.randint(1, 300)}"
            email = f"email_{random.randint(1, 300)}@example.com"
            timestamp = self.generate_timestamp(i)
            cursor.execute(f'INSERT INTO {table_name} (name, email, timestamp) VALUES (?, ?, ?)',
                           (name, email, timestamp))

        # Commit the changes
        connection.commit()

    def create_tables(self):
        # Create table1
        self.create_table(self.conn1, self.cursor1, 'table1', 30)

        # Create table2
        self.create_table(self.conn2, self.cursor2, 'table2',10)
