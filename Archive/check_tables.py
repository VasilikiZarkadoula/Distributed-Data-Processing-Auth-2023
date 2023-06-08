import logging


class CheckTables:
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

    def get_table_name(self, cursor):
        """
        Get the table name from the cursor.

        :param cursor: Database cursor
        :return: Table name
        """
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        result = cursor.fetchone()
        return result[0] if result else None

    def check_table(self, conn, cursor):
        """
        Check if a table in a SQLite database is filled.

        Args:
            database_file (str): The filename of the SQLite database file.
            table_name (str): The name of the table to check.

        Returns:
            bool: True if the table is filled, False otherwise.
        """

        table_name = self.get_table_name(cursor)

        # Execute a query to fetch all rows from the table
        cursor.execute(f'SELECT * FROM {table_name}')
        rows = cursor.fetchall()

        # Check if the table is filled
        if len(rows) > 0:
            logging.info(f"{table_name}")
            # Print the retrieved data
            for row in rows:
                logging.info(row)
            return True
        else:
            logging.info(f"{table_name} is empty.")
            return False

    def check_all_tables(self):
        """
        Check all tables in the SQLite database files.

        Prints information about the tables, including the retrieved data if the tables are filled.
        """
        self.check_table(self.conn1, self.cursor1)
        self.check_table(self.conn2, self.cursor2)
