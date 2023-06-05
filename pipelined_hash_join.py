import sqlite3
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(message)s')

class HashJoin:
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
        self.ht1 = {}  # Initialize empty hash table for table1
        self.ht2 = {}  # Initialize empty hash table for table2
        self.counter = 0  # Initialize counter for counting the mathing records

    def get_table_name(self, cursor):
        """
        Get the table name from the cursor.

        :param cursor: Database cursor
        :return: Table name
        """
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        result = cursor.fetchone()
        return result[0] if result else None

    @staticmethod
    def probe_and_insert(tuple_, ht_probe, ht_insert):
        """
        Perform probing and insertion in the hash tables.

        :param tuple_: Tuple representing a record from one of the databases
        :param ht_probe: Hash table for probing
        :param ht_insert: Hash table for insertion
        :return: Result set if a match is found within the specified timestamp difference, otherwise None
        """
        probe_result_key = tuple_[0]  # Define the key of the tuple (join attribute)
        result_set = None

        if probe_result_key in ht_probe:
            # Retrieve matching records from both databases using the probe result key
            record1 = ht_probe[probe_result_key]
            record2 = tuple_
            timestamp1 = record1[3]  # Extract the timestamp from record1
            timestamp2 = record2[3]  # Extract the timestamp from record2

            # Check if the timestamp difference is less than 12 hours
            time_diff = (abs(datetime.strptime(timestamp1, "%Y-%m-%d %H:%M:%S") - datetime.strptime(timestamp2,
                                                                                                    "%Y-%m-%d %H:%M:%S"))).total_seconds() / 3600
            if time_diff < 12:
                result_set = (probe_result_key, record1, record2, round(time_diff))

        ht_insert[probe_result_key] = tuple_  # Insert the tuple into the insertion hash table

        return result_set

    def perform_pipelined_hash_join(self):
        """
        Perform the double pipelined hash join algorithm.

        It iterates over the tuples from both databases, performs probing and insertion in the hash tables, and prints
        the join results.

        The method follows the pipelined hash join algorithm, which involves alternating between reading tuples from
        the databases, probing one hash table, and inserting tuples into the other hash table.

        The join results are printed using the process_join_result method.
        """

        table1_name = self.get_table_name(self.cursor1)
        table2_name = self.get_table_name(self.cursor2)

        self.cursor1.execute(f"SELECT * FROM {table1_name}")
        self.cursor2.execute(f"SELECT * FROM {table2_name}")

        read_from_database1 = True  # Flag to determine database to read from

        logging.info("\n============================== Pipelined Hash Join ==============================")

        # Iterate over the tuples from both databases and perform the pipelined hash join operation
        while True:
            if read_from_database1:
                tuple_ = self.cursor1.fetchone()
                if tuple_ is None:
                    break

                # join the tuple from table1 with the probed tuples of table2
                result = self.probe_and_insert(tuple_, self.ht2, self.ht1)
                self.process_join_result(result, self.conn2.execute("PRAGMA database_list").fetchall()[0][1],
                                         self.conn1.execute("PRAGMA database_list").fetchall()[0][1])
            else:
                tuple_ = self.cursor2.fetchone()
                if tuple_ is None:
                    break

                result = self.probe_and_insert(tuple_, self.ht1, self.ht2)
                self.process_join_result(result, self.conn1.execute("PRAGMA database_list").fetchall()[0][1],
                                         self.conn2.execute("PRAGMA database_list").fetchall()[0][1])

            read_from_database1 = not read_from_database1  # Switch the flag to alternate between databases

        # Process the remaining tuples from database1 (if any)
        while True:
            tuple_ = self.cursor1.fetchone()
            if tuple_ is None:
                break
            # probe them against table2 and output
            result = self.probe_and_insert(tuple_, self.ht2, self.ht1)
            self.process_join_result(result, self.conn2.execute("PRAGMA database_list").fetchall()[0][1],
                                     self.conn1.execute("PRAGMA database_list").fetchall()[0][1])

        # Process the remaining tuples from database2 (if any)
        while True:
            tuple_ = self.cursor2.fetchone()
            if tuple_ is None:
                break

            # probe them against table1 and output
            result = self.probe_and_insert(tuple_, self.ht1, self.ht2)
            self.process_join_result(result, self.conn1.execute("PRAGMA database_list").fetchall()[0][1],
                                         self.conn2.execute("PRAGMA database_list").fetchall()[0][1])

    def process_join_result(self, triple, probe, insert):
        """
        Print the join result.
        """
        if triple is not None:
            logging.info(f"Matching records from probing {probe} and inserting {insert} => {triple}")
            self.counter += 1


