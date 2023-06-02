import sqlite3
from datetime import datetime, timedelta
import logging
import time

#logging.basicConfig(level=logging.INFO, format='%(message)s')


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

    def get_largest_table(self):
        """
        Determine the largest table between two databases.

        Returns:
            sqlite3.Connection: The connection to the database containing the largest table.
        """
        self.cursor1.execute("SELECT COUNT(*) FROM table1")
        count_table1 = self.cursor1.fetchone()[0]

        self.cursor2.execute("SELECT COUNT(*) FROM table2")
        count_table2 = self.cursor2.fetchone()[0]

        if count_table1 >= count_table2:
            self.logger.info("table1 in database1 is the largest")
            return self.conn1
        else:
            self.logger.info("table2 in database2 is the largest")
            return self.conn2

    def perform_semi_join(self):
        """
        Perform the semi-join operation between two databases.

        Steps:
        1. Determine the largest table between the two databases.
        2. Compute S1 = Πa(S), where S is the largest table.
        3. Compute R1 = R ⋉aS1, where R is the smaller table.
        4. Compute R1⋈aS, which represents the semi-join result.
        5. Compare the timestamps of matching rows and output the results.

        Note:
        - Πa(S) denotes the projection operation on table S, selecting only the 'id' (a) column.
        - R ⋉aS1 denotes the semi-join operation between R and S1, where 'a' represents the join attribute.
        - R1⋈aS represents the join operation between R1 and S, where 'a' represents the join attribute.

        After performing the semi-join, the matching results are logged and printed, and the counter is incremented.
        """
        self.logger.info("============================== Semi Join ==============================")

        # Following Semi-join-based Ordering from slide 44 - Ozsu-QP-2022

        S = self.get_largest_table()
        R = self.conn1 if S == self.conn2 else self.conn2

        cursor_S = S.cursor()
        cursor_R = R.cursor()

        cursor_S.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        cursor_R.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        table_S_name = cursor_S.fetchone()[0]
        self.logger.info(f"S = {table_S_name}")
        table_R_name = cursor_R.fetchone()[0]
        self.logger.info(f"R = {table_R_name}")

        # step 1 - S1 = Πa(S)
        cursor_S.execute(f"SELECT id FROM {table_S_name}")
        S1 = [row[0] for row in cursor_S.fetchall()]

        self.logger.info("Transferring S1 to Site 1...")

        # step 2 - Site 1 computes R1 = R ⋉aS1
        #print("Computing R1 = R ⋉aS1")
        self.logger.info("Computing R1 = R semi-join S1")
        cursor_R.execute(f"SELECT * FROM {table_R_name}")
        R1 = [row for row in cursor_R.fetchall() if row[0] in S1]  # List comprehension

        self.logger.info("Transferring R1 to Site 2...")

        # step 3 - Site 2 computes R1⋈aS
        #print("Computing R1⋈aS")
        self.logger.info("Computing R1 join S")

        for row_R1 in R1:
            cursor_S.execute(f"SELECT name, email, timestamp FROM {table_S_name} WHERE id=?", (row_R1[0],))
            row_S = cursor_S.fetchone()
            if row_S:
                timediff = abs(datetime.strptime(row_R1[3], "%Y-%m-%d %H:%M:%S") - datetime.strptime(row_S[2],"%Y-%m-%d %H:%M:%S"))
                if timediff <= timedelta(hours=12):
                    self.logger.info(f"Got match => ({row_R1[0]}, ({row_R1[1]}, {row_R1[2]}, {row_R1[3]}), {row_S}, {round(timediff.total_seconds() / 3600)})")
                    self.counter += 1


logging.basicConfig(filename='semi_join_logs.log', level=logging.INFO)

database_path = 'databases/'

# Connect to the two databases
conn1 = sqlite3.connect(database_path + "database1.db")
conn2 = sqlite3.connect(database_path + "database2.db")

# Create an instance of SemiJoin
semi_join = SemiJoin(conn1, conn2)

start_time = time.time()
# Perform the semi-join
semi_join.perform_semi_join()

end_time = time.time()
running_time = end_time - start_time
logging.info(f"Running time: {running_time} seconds")

# Get the count of matched results
logging.info(f"Total matches: {semi_join.counter}")

# Close the database connections
conn1.close()
conn2.close()
