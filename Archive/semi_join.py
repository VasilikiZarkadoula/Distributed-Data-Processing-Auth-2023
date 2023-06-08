import sqlite3
from datetime import datetime, timedelta
import logging
import time

class SemiJoin:
    def __init__(self, conn1, conn2):
        """
        Initialize the SemiJoin class.

        Args:
            conn1 (sqlite3.Connection): Connection to the first database.
            conn2 (sqlite3.Connection): Connection to the second database.
        """
        self.conn1 = conn1
        self.conn2 = conn2
        self.cursor1 = conn1.cursor()
        self.cursor2 = conn2.cursor()
        self.counter = 0
        self.logger = logging.getLogger("SemiJoin")
        self.logger.setLevel(logging.INFO)

    def get_table_name(self, cursor):
        """
        Get the table name from the cursor.

        Args:
            cursor (sqlite3.Cursor): Database cursor.

        Returns:
            str: Table name.
        """
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        result = cursor.fetchone()
        return result[0] if result else None

    def get_largest_table(self):
        """
        Determine the largest table between two databases.

        Returns:
            sqlite3.Connection: The connection to the database containing the largest table.
        """
        table1 = self.get_table_name(self.cursor1)
        self.cursor1.execute(f"SELECT COUNT(*) FROM {table1}")
        count_table1 = self.cursor1.fetchone()[0]

        table2 = self.get_table_name(self.cursor2)
        self.cursor2.execute(f"SELECT COUNT(*) FROM {table2}")
        count_table2 = self.cursor2.fetchone()[0]

        if count_table1 >= count_table2:
            self.logger.info("table in database1 is the largest")
            return self.conn1
        else:
            self.logger.info("table in database2 is the largest")
            return self.conn2

    def perform_semi_join(self):
        """
        Perform the semi-join operation between two databases.

        Steps:
        1. Determine the largest table between the two databases.
        2. Compute S1 = Πa(S), where S is the largest table.
        3. Compute R1 = R ⋉aS1, where R is the smaller table.
        5. Compare the timestamps of matching rows and output the results.

        Note:
        - Πa(S) denotes the projection operation on table S, selecting only the 'id' (a) column.
        - R ⋉aS1 denotes the semi-join operation between R and S1, where 'a' represents the join attribute.

        After performing the semi-join, the matching results are logged and printed, and the counter is incremented.
        """
        self.logger.info("\n============================== Semi Join ==============================")

        # Following Semi-join-based Ordering from slide 44 - Ozsu-QP-2022

        S = self.get_largest_table()
        R = self.conn1 if S == self.conn2 else self.conn2

        cursor_S = S.cursor()
        cursor_R = R.cursor()

        table_S_name = self.get_table_name(cursor_S)
        self.logger.info(f"S = {table_S_name}")
        table_R_name = self.get_table_name(cursor_R)
        self.logger.info(f"R = {table_R_name}")

        # step 1 - S1 = Πa(S)
        cursor_S.execute(f"SELECT id FROM {table_S_name}")
        S1 = [row[0] for row in cursor_S.fetchall()]

        # step 2 - Site 1 computes R1 = R ⋉aS1
        self.logger.info("Computing R1 = R semi-join S1:")
        cursor_R.execute(f"SELECT * FROM {table_R_name}")
        R1 = []
        for row in cursor_R.fetchall():
            cursor_S.execute(f"SELECT name, email, timestamp FROM {table_S_name} WHERE id=?", (row[0],))
            row_S = cursor_S.fetchone()
            timediff = abs(datetime.strptime(row[3], "%Y-%m-%d %H:%M:%S") - datetime.strptime(row_S[2], "%Y-%m-%d %H:%M:%S"))
            if (row[0] in S1) and (timediff <= timedelta(hours=12)):
                logging.info(row)
                R1.append(row)
                self.counter += 1
