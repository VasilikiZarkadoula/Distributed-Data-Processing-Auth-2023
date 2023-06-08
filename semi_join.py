from datetime import datetime, timedelta
import logging


class SemiJoin:
    def __init__(self, table1, table2, table1_name, table2_name, timestamp_diff, lazy):
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
        self.counter = 0
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
            self.logger.info(f"{self.table1_name} is the largest")
            return self.table1
        else:
            self.logger.info(f"{self.table2_name} is the largest")
            return self.table2

    def perform_semi_join(self):
        """
        Perform the semi-join operation.

        :return: The result of the semi-join operation.
        """
        self.logger.info("\n============================== Semi Join ==============================")

        S = self.get_largest_table()
        R = self.table1 if S == self.table2 else self.table2

        table_S_name = self.table1_name if S == self.table1 else self.table2_name
        self.logger.info(f"S = {table_S_name}")
        table_R_name = self.table1_name if R == self.table1 else self.table2_name
        self.logger.info(f"R = {table_R_name}")

        self.logger.info("Computing R1 = R semi-join S1:")
        R1 = []
        S_lookup = {row[0]: row for row in S}
        for row in R:
            row_R_id = row[0]
            if row_R_id in S_lookup:
                if self.lazy:
                    row_S = S_lookup[row_R_id]
                    # Calculate the timestamp difference in hours
                    time_diff = (abs(datetime.strptime(row[3], "%Y-%m-%d %H:%M:%S")
                                     - datetime.strptime(row_S[3], "%Y-%m-%d %H:%M:%S"))).total_seconds() / 3600
                    if time_diff < self.timestamp_diff:
                        # Add the row to the result set
                        R1.append(row)
                        # self.logger.info(row)
                        self.counter += 1
                else:
                    # Add the row to the result set
                    R1.append(row)
                    # self.logger.info(row)
                    self.counter += 1

        return R1
