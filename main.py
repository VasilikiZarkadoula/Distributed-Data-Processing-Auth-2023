import sqlite3
import time
import logging
from create_table import CreateTable
from check_tables import CheckTables
from semi_join import SemiJoin
from pipelined_hash_join import HashJoin
from datetime import datetime
import pandas as pd

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
CreateTable(conn1, conn2).create_tables(seed=0)

# Check tables
check_tables = CheckTables(conn1, conn2)
table1, table1_name, table2, table2_name = check_tables.check_tables()


timestamp_diff = 12 # in hours

# Perform the pipelined hash join eager
start_time = time.time()
filtered_table1, filtered_table2 = filter_tables(table1, table2, timestamp_diff)
hash_join_eager = HashJoin(list(filtered_table1), list(filtered_table2), table1_name, table2_name, timestamp_diff, False)
hash_join_eager.perform_pipelined_hash_join()
end_time = time.time()
running_time = end_time - start_time

logging.info(f"\nTotal matches (pipelined hash join eager): {hash_join_eager.counter}\n")
logging.info(f"Running time (pipelined hash join eager): {running_time} seconds\n")

# Perform the pipelined hash join lazy
hash_join = HashJoin(table1, table2, table1_name, table2_name, timestamp_diff, True)
start_time = time.time()
hash_join.perform_pipelined_hash_join()
end_time = time.time()
running_time = end_time - start_time

logging.info(f"\nTotal matches (pipelined hash join lazy): {hash_join.counter}\n")
logging.info(f"Running time (pipelined hash join lazy): {running_time} seconds\n")

# Perform the semi-join eager
start_time = time.time()
filtered_table1, filtered_table2 = filter_tables(table1, table2, timestamp_diff)
semi_join_eager = SemiJoin(list(filtered_table1), list(filtered_table2), table1_name, table2_name, timestamp_diff, False)
semi_join_eager.perform_semi_join()
end_time = time.time()
running_time = end_time - start_time

logging.info(f"\nTotal matches (semi-join eager): {semi_join_eager.counter}\n")
logging.info(f"Running time (semi-join eager): {running_time} seconds")

# Perform the semi-join lazy
semi_join = SemiJoin(table1, table2, table1_name, table2_name, timestamp_diff, True)
start_time = time.time()
semi_join.perform_semi_join()
end_time = time.time()
running_time = end_time - start_time

logging.info(f"\nTotal matches (semi-join-lazy): {semi_join.counter}\n")
logging.info(f"Running time (semi-join-lazy): {running_time} seconds")

df1 = pd.DataFrame(filtered_table1)
df2 = pd.DataFrame(filtered_table2)
# Print the DataFrame
df1.to_csv('filt1.csv', index=False)
df2.to_csv('filt2.csv', index=False)
# Close the database connections
conn1.close()
conn2.close()
