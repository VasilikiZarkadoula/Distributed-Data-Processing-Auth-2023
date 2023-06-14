import logging


class CheckTables:
    def __init__(self, conn1, conn2):
        """
        Initialize the CheckTables object.

        :param conn1: Connection to the first database
        :param conn2: Connection to the second database
        """
        self.conn1 = conn1
        self.conn2 = conn2
        self.cursor1 = conn1.cursor()
        self.cursor2 = conn2.cursor()

    @staticmethod
    def get_table_name(cursor):
        """
        Get the table name from the cursor.

        :param cursor: Database cursor
        :return: Table name
        """
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        result = cursor.fetchone()
        return result[0] if result else None

    @staticmethod
    def check_table(table, table_name):
        """
        Check if a table is empty and print its contents if it is not empty.

        :param table: The table to check
        :param table_name: The name of the table
        """
        if len(table) > 0:
            logging.info(f"{table_name}")
            # Log the retrieved data
            for row in table:
                logging.info(row)
        else:
            logging.info(f"{table_name} is empty.")

    def check_tables(self):
        """
        Check the tables in the connected databases.

        :return: The retrieved tables as tuples
        """

        table1_name = self.get_table_name(self.cursor1)
        table2_name = self.get_table_name(self.cursor2)

        self.cursor1.execute(f"SELECT * FROM {table1_name}")
        self.cursor2.execute(f"SELECT * FROM {table2_name}")

        table1 = self.cursor1.fetchall()
        table2 = self.cursor2.fetchall()

        # Check if the tables are filled
        self.check_table(table1, table1_name)
        self.check_table(table2, table2_name)

        return table1, table1_name, table2, table2_name
