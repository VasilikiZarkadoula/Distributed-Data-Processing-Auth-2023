import sqlite3
import time
import logging
from create_table import CreateTable
from check_tables import CheckTables
from semi_join import SemiJoin
from pipelined_hash_join import HashJoin
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(message)s')


def filter_tables(table1, table2, timestamp_diff):
    filtered_table1 = set()
    filtered_table2 = set()

    for row1 in table1:
        timestamp1 = datetime.strptime(row1[3], "%Y-%m-%d %H:%M:%S")  # Access timestamp using index
        for row2 in table2:
            timestamp2 = datetime.strptime(row2[3], "%Y-%m-%d %H:%M:%S")  # Access timestamp using index
            if abs((timestamp1 - timestamp2).total_seconds() / 3600) < timestamp_diff:
                filtered_table1.add(tuple(row1))
                break

    for row2 in table2:
        timestamp2 = datetime.strptime(row2[3], "%Y-%m-%d %H:%M:%S")  # Access timestamp using index
        for row1 in table1:
            timestamp1 = datetime.strptime(row1[3], "%Y-%m-%d %H:%M:%S")  # Access timestamp using index
            if abs((timestamp2 - timestamp1).total_seconds() / 3600) < timestamp_diff:
                filtered_table2.add(tuple(row2))
                break

    return filtered_table1, filtered_table2


# Connect to the two databases
conn1 = sqlite3.connect('databases/database1.db')
conn2 = sqlite3.connect('databases/database2.db')

# Create tables
size_table_1 = 30
size_table_2 = 20
CreateTable(conn1, conn2).create_tables(size_table_1, size_table_2, seed=0)

# Check tables
check_tables = CheckTables(conn1, conn2)
table1, table1_name, table2, table2_name = check_tables.check_tables()


timestamp_diff = 6  # in hours


############################ pipelined hash join #################################

# Perform the pipelined hash join - Filter-Timestamps-Then-Join (FTTJ)
start_time = time.time()
filtered_table1, filtered_table2 = filter_tables(table1, table2, timestamp_diff)
hash_join_eager = HashJoin(list(filtered_table1), list(filtered_table2), table1_name, table2_name, timestamp_diff, False)
hash_join_eager.perform_pipelined_hash_join()
end_time = time.time()
running_time = end_time - start_time

logging.info(f"\nTotal matches (pipelined hash join FTTJ): {hash_join_eager.counter}\n")
logging.info(f"Running time (pipelined hash join FTTJ): {running_time} seconds\n")

# Perform the pipelined hash join lazy
hash_join = HashJoin(table1, table2, table1_name, table2_name, timestamp_diff, True)
start_time = time.time()
hash_join.perform_pipelined_hash_join()
end_time = time.time()
running_time = end_time - start_time

logging.info(f"\nTotal matches (pipelined hash join lazy): {hash_join.counter}\n")
logging.info(f"Running time (pipelined hash join lazy): {running_time} seconds\n")

################################ semi join ######################################

# Perform the semi-join - Filter-Timestamps-Then-Join (FTTJ)
start_time = time.time()
filtered_table1, filtered_table2 = filter_tables(table1, table2, timestamp_diff)
semi_join = SemiJoin(list(filtered_table1), list(filtered_table2), table1_name, table2_name, timestamp_diff, False, False)
semi_join.perform_semi_join()
end_time = time.time()
running_time = end_time - start_time

logging.info(f"\nTotal matches (semi-join - FTTJ): {semi_join.counter}\n")
logging.info(f"Running time (semi-join - FTTJ): {running_time} seconds")

# Perform the semi-join eager
semi_join_eager = SemiJoin(table1, table2, table1_name, table2_name, timestamp_diff, False, True)
start_time = time.time()
_, size_used = semi_join_eager.perform_semi_join()
end_time = time.time()
running_time = end_time - start_time

logging.info(f"\nTotal matches (semi-join eager): {semi_join_eager.counter}\n")
logging.info(f"Running time (semi-join eager): {running_time} seconds")
logging.info(f"Total size used (semi-join eager): {size_used} MB")

# Perform the semi-join lazy
semi_join_lazy = SemiJoin(table1, table2, table1_name, table2_name, timestamp_diff, True, False)
start_time = time.time()
_, size_used = semi_join_lazy.perform_semi_join()
end_time = time.time()
running_time = end_time - start_time

logging.info(f"\nTotal matches (semi-join lazy): {semi_join_lazy.counter}\n")
logging.info(f"Running time (semi-join lazy): {running_time} seconds")
logging.info(f"Total size used (semi-join lazy): {size_used} MB")


# Close the database connections
conn1.close()
conn2.close()
