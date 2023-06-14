from datetime import datetime
import logging
import sys


class SemiJoin:
    def __init__(self, table1, table2, table1_name, table2_name, timestamp_diff, lazy, eager):
        """
        Initialize the SemiJoin object.

        :param table1: The first table.
        :param table2: The second table.
        :param lazy: A flag indicating lazy evaluation.
        """
        self.table1 = table1
        self.table2 = table2
        self.table1_name = table1_name
        self.table2_name = table2_name
        self.timestamp_diff = timestamp_diff
        self.lazy = lazy
        self.eager = eager
        self.counter = 0 # Initialize counter for counting the matching records
        self.logger = logging.getLogger("SemiJoin")
        self.logger.setLevel(logging.INFO)

    def get_largest_table(self):
        """
        Determine the largest table between table1 and table2.

        :return: The largest table.
        """
        count_table1 = len(self.table1)
        count_table2 = len(self.table2)

        if count_table1 >= count_table2:
            self.logger.info(f"\n{self.table1_name} is the largest")
            return self.table1
        else:
            self.logger.info(f"{self.table2_name} is the largest")
            return self.table2

    def perform_semi_join(self):
        """
        Perform the semi-join operation between two tables.

        Steps:
        1. Determine the largest table between table1 and table2.
        2. Compute S1 based on the chosen strategy (lazy, eager, or join operation after tables are filtered).
        3. Compute R1 = R semi-join S1 based on the chosen strategy.
        4. Return the result of the semi-join operation and the memory size used.

        Strategies:
        - Lazy :
            - S_lookup: an index-like structure in the form of a dictionary that maps the 'id' values to their
                        corresponding 'timestamp' values.
            - R1 = R semi-join S_lookup:
                - For each row in R, check if the id exists in S_lookup.
                - If it exists, calculate the timestamp difference between R and S1.
                - If the difference is less than the specified timestamp threshold, add the row to R1.

        - Eager :
              Now, instead of fetching rows from R one at a time, all rows from R are fetched in advance.
            - S_lookup: an index-like structure in the form of a dictionary that maps the 'id' values to their
                        corresponding 'timestamp' values.
            - R_lookup: an index-like structure in the form of a dictionary that maps the 'id' values to their
                        corresponding row values from R table.
            - R1 = R_lookup semi-join S_lookup:
                - For each key in R_lookup, check if it exists in S_lookup.
                - If it exists, calculate the timestamp difference between R_lookup and S_lookup.
                - If the difference is less than the specified timestamp threshold, add the corresponding row from R to R1.

        - Filtering timestamps before joinning on id:
            - S_lookup: an index-like structure in the form of a dictionary that maps the 'id' values to their
                        corresponding 'timestamp' values.
            - R1 = R semi-join S_lookup:
                - For each row in R, check if the id exists in S_lookup.
                - If it exists, add the row to R1.

        Returns:
        - R1: The result of the semi-join operation.
        - size_used: The memory size (in MB) used by the result and lookup dictionaries.
        """

        S = self.get_largest_table()
        R = self.table1 if S == self.table2 else self.table2

        table_S_name = self.table1_name if S == self.table1 else self.table2_name
        self.logger.info(f"S = {table_S_name}")
        table_R_name = self.table1_name if R == self.table1 else self.table2_name
        self.logger.info(f"R = {table_R_name}")

        self.logger.info("Computing R1 = R semi-join S1:")

        R1 = []

        # Lazy
        if self.lazy:
            S_lookup = {row[0]: row[3] for row in S}
            self.logger.info("\n============================== Semi Join Lazy ==============================")
            for row in R:
                row_R_id = row[0]
                if row_R_id in S_lookup:
                    timestamp_S = S_lookup[row_R_id]
                    # Calculate the timestamp difference in hours
                    time_diff = (abs(datetime.strptime(row[3], "%Y-%m-%d %H:%M:%S")
                                     - datetime.strptime(timestamp_S, "%Y-%m-%d %H:%M:%S"))).total_seconds() / 3600
                    if time_diff < self.timestamp_diff:
                        # Add the row to the result set
                        R1.append(row)
                        self.logger.info(row)
                        self.counter += 1

            size_used = (sys.getsizeof(R1) + sys.getsizeof(S_lookup)) / 1024 / 1024

        # Eager
        elif self.eager:
            self.logger.info("\n============================== Semi Join Eager ==============================")
            S_lookup = {row[0]: (row[3]) for row in S}
            R_lookup = {row[0]: row for row in R}
            for key in R_lookup.keys():
                if key in S_lookup:
                    rowR = R_lookup[key]
                    time_diff = (abs(datetime.strptime(rowR[3], "%Y-%m-%d %H:%M:%S")
                                     - datetime.strptime(S_lookup[key], "%Y-%m-%d %H:%M:%S"))).total_seconds() / 3600
                    if time_diff < self.timestamp_diff:
                        # Add the row to the result set
                        R1.append(rowR)
                        self.logger.info(rowR)
                        self.counter += 1

            size_used = (sys.getsizeof(R1) + sys.getsizeof(S_lookup) + sys.getsizeof(R_lookup)) / 1024 / 1024

        # Check only id -  timestamps are filtered before join
        else:
            self.logger.info("\n============================== Semi Join FTTJ ==============================")
            S_lookup = {row[0]: row[3] for row in S}
            for row in R:
                row_R_id = row[0]
                if row_R_id in S_lookup:
                    # Add the row to the result set
                    R1.append(row)
                    self.logger.info(row)
                    self.counter += 1

            size_used = (sys.getsizeof(R1) + sys.getsizeof(S_lookup)) / 1024 / 1024

        return R1, size_used
