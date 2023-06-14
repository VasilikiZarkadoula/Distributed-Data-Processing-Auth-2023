import logging
from datetime import datetime


class HashJoin:
    def __init__(self, table1, table2, table1_name, table2_name, timestamp_diff, lazy):
        self.table1 = table1
        self.table2 = table2
        self.table1_name = table1_name
        self.table2_name = table2_name
        self.timestamp_diff = timestamp_diff
        self.lazy = lazy
        self.ht1 = {}  # Initialize empty hash table for table1
        self.ht2 = {}  # Initialize empty hash table for table2
        self.counter = 0  # Initialize counter for counting the matching records
        self.logger = logging.getLogger("PipelinedHashJoin")
        self.logger.setLevel(logging.INFO)

    def probe_and_insert(self, tuple_, ht_probe, ht_insert):
        """
        Perform probing and insertion in the hash tables.

        :param tuple_: Tuple representing a record from one of the databases
        :param ht_probe: Hash table for probing
        :param ht_insert: Hash table for insertion
        :return: Result set if a match is found within the specified timestamp difference if lazy evaluation
        """
        probe_result_key = tuple_[0]  # Define the key of the tuple (join attribute)
        result_set = None

        if probe_result_key in ht_probe:
            # Retrieve matching records from both databases using the probe result key
            record1 = ht_probe[probe_result_key]
            record2 = tuple_

            # Lazy
            if self.lazy:
                # Check if the timestamp difference is less than a specified hour limit
                time_diff = (abs(datetime.strptime(record1[3], "%Y-%m-%d %H:%M:%S") -
                                    datetime.strptime(record2[3], "%Y-%m-%d %H:%M:%S"))).total_seconds() / 3600
                if time_diff < self.timestamp_diff:
                    result_set = (probe_result_key, record1, record2)

            else:  # Check only id -  timestamps are filtered before join
                result_set = (probe_result_key, record1, record2)

        ht_insert[probe_result_key] = tuple_  # Insert the tuple into the insertion hash table

        return result_set

    def perform_pipelined_hash_join(self):
        """
        Perform the double pipelined hash join algorithm.

        It iterates over the tuples from both lists, performs probing and insertion in the hash tables, and prints
        the join results.

        The method follows the pipelined hash join algorithm, which involves alternating between reading tuples from
        the lists, probing one hash table, and inserting tuples into the other hash table.

        The join results are printed using the process_join_result method.
        """

        read_index_table1 = 0  # Index for reading from table1
        read_index_table2 = 0  # Index for reading from table2

        self.logger.info("\n============================== Pipelined Hash Join ==============================")

        # Iterate over the tuples from both tables and perform the pipelined hash join operation
        while True:
            if read_index_table1 < len(self.table1):
                # Read a tuple from table1 at the current read index
                tuple_ = self.table1[read_index_table1]

                # Perform probing and insertion by using table2 as the probe hash table
                # and table1 as the insert hash table
                result = self.probe_and_insert(tuple_, self.ht2, self.ht1)

                # Process the join result
                self.process_join_result(result, self.table2_name, self.table1_name)

                # Increment the read index for table1
                read_index_table1 += 1

            if read_index_table2 < len(self.table2):
                # Read a tuple from table2 at the current read index
                tuple_ = self.table2[read_index_table2]

                # Perform probing and insertion by using table1 as the probe hash table
                # and table2 as the insert hash table
                result = self.probe_and_insert(tuple_, self.ht1, self.ht2)

                # Process the join result
                self.process_join_result(result, self.table1_name, self.table2_name)

                # Increment the read index for table2
                read_index_table2 += 1

            # Check if we have processed all tuples from both tables
            if read_index_table1 >= len(self.table1) and read_index_table2 >= len(self.table2):
                break

        # Process the remaining tuples from table1 (if any)
        while read_index_table1 < len(self.table1):
            # Read a tuple from table1 at the current read index
            tuple_ = self.table1[read_index_table1]

            # Perform probing and insertion by using table2 as the probe hash table and table1 as the insert hash table
            result = self.probe_and_insert(tuple_, self.ht2, self.ht1)

            # Process the join result
            self.process_join_result(result, self.table2_name, self.table1_name)

            # Increment the read index for table1
            read_index_table1 += 1

        # Process the remaining tuples from table2 (if any)
        while read_index_table2 < len(self.table2):
            # Read a tuple from table2 at the current read index
            tuple_ = self.table2[read_index_table2]

            # Perform probing and insertion by using table1 as the probe hash table and table2 as the insert hash table
            result = self.probe_and_insert(tuple_, self.ht1, self.ht2)

            # Process the join result
            self.process_join_result(result, self.table1_name, self.table2_name)

            # Increment the read index for table2
            read_index_table2 += 1

    def process_join_result(self, triple, probe, insert):
        """
        Print the join result.
        """
        if triple is not None:
            self.logger.info(f"Matching records from probing {probe} and inserting {insert} => {triple}")
            #self.logger.info(triple)
            self.counter += 1
