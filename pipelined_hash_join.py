import sqlite3
from datetime import datetime, timedelta
import logging
import time

#logging.basicConfig(level=logging.INFO, format='%(message)s')

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
        self.hash_table = {}
        self.counter = 0

    def retrieve_matching_records(self, tuple_):
        """
        Retrieve matching records from the hash table.

        :param tuple_: Database table tuple
        :yield: Tuple representing the join result (key, record1, record2, time_diff)

        This method takes a tuple from a database table and performs a probing operation by checking if the hash table
        already contains a specific key. If the key is present, it yields the matching records from both databases.
        If the key is not present, it inserts the key into the hash table and continues processing.

        The yield statement allows the method to be used as a generator, yielding the join results one at a time. This
        reduces memory consumption and improves scalability for large datasets.
        """
        key = tuple_[0]
        timestamp = tuple_[3]

        # Check for key-based matches and timestamp-based matches
        for record_key, records in self.hash_table.items():
            for record in records:
                record_timestamp = record[3]
                time_diff = (abs(datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S") - datetime.strptime(record_timestamp, "%Y-%m-%d %H:%M:%S"))).total_seconds() / 3600
                if key == record[0] and time_diff <= 12:
                    yield (key, record, tuple_, round(time_diff))

        # Add the current tuple to the hash table
        self.hash_table.setdefault(key, []).append(tuple_)


    def perform_pipelined_hash_join(self):
        """
        Perform the pipelined hash join algorithm.

        This method implements the main logic of the pipelined hash join algorithm. It iterates over the tuples from
        both databases, performs probing and insertion in the hash table, and prints the join results.

        The method follows the pipelined hash join algorithm, which involves alternating between reading tuples from
        the databases, probing the hash table, and inserting tuples into the hash table. It ensures efficient join
        operation and reduces memory usage.

        The join results are printed using the process_join_result method, and the total number of matches is tracked by
        the counter attribute.
        """

        self.cursor1.execute("SELECT * FROM table1")
        self.cursor2.execute("SELECT * FROM table2")
        read_from_database1 = True  # Flag to indicate if we are currently reading from database1
        logging.info(f"Reading from database1: {read_from_database1}")
        logging.info("============================== Pipelined Hash Join ==============================")

        # Iterate over the tuples from both databases and perform the hash join operation
        while True:
            if read_from_database1:  # If reading from database1
                tuple_ = self.cursor1.fetchone()
                if tuple_ is None:
                    break
                # Perform probing and insertion, and iterate over the join results
                for result in self.retrieve_matching_records(tuple_):
                    self.process_join_result(result, "database2", "database1")
            else:  # If reading from database2
                tuple_ = self.cursor2.fetchone()
                if tuple_ is None:
                    break
                # Perform probing and insertion, and iterate over the join results
                for result in self.retrieve_matching_records(tuple_):
                    self.process_join_result(result, "database1", "database2")

            read_from_database1 = not read_from_database1  # Switch the flag to alternate between databases

        # Process the remaining tuples from database1 (if any)
        while True:
            tuple_ = self.cursor1.fetchone()
            if tuple_ is None:
                break
            # Perform probing and insertion, and iterate over the join results
            for result in self.retrieve_matching_records(tuple_):
                self.process_join_result(result, "database2", "database1")

        # Process the remaining tuples from database2 (if any)
        while True:
            tuple_ = self.cursor2.fetchone()
            if tuple_ is None:
                break
            # Perform probing and insertion, and iterate over the join results
            for result in self.retrieve_matching_records(tuple_):
                self.process_join_result(result, "database1", "database2")

    def process_join_result(self, triple, probe, insert):
        """
        Print the join result.

        :param triple: Tuple representing the join result (key, record1, record2)
        :param probe: Name of the database where the probing occurred
        :param insert: Name of the database where the insertion occurred

        This method prints the join result, which is a tuple containing the key and the matching records from both
        databases. The `probe` parameter specifies the name of the database where the probing operation occurred,
        and the `insert` parameter specifies the name of the database where the insertion operation occurred.

        Example output: "Matching records from probing database2 and inserting database1 => (key, record1, record2)"
        """
        if triple is not None:
            logging.info(f"Matching records from probing {probe} and inserting {insert} => {triple}")
            self.counter += 1


logging.basicConfig(filename='pipelined_hash_join_logs.log', level=logging.INFO)

database_path = 'databases/'

# Connect to the two databases
conn1 = sqlite3.connect(database_path + "database1.db")
conn2 = sqlite3.connect(database_path + "database2.db")

# Create instances of HashJoin with the database connections
hash_join = HashJoin(conn1, conn2)

start_time = time.time()
# Perform the pipelined hash join
hash_join.perform_pipelined_hash_join()

end_time = time.time()
running_time = end_time - start_time
logging.info(f"Running time: {running_time} seconds")

# Print the total number of matches found
logging.info(f"Total matches found: {hash_join.counter}")

# Close the database connections
conn1.close()
conn2.close()
