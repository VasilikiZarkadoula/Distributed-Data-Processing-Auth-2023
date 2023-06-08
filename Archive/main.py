import sqlite3
import time
import logging
from create_table import CreateTable
from check_tables import CheckTables
from semi_join import SemiJoin
from pipelined_hash_join import HashJoin

logging.basicConfig(level=logging.INFO, format='%(message)s')

# Connect to the two databases
conn1 = sqlite3.connect('databases/database1.db')
conn2 = sqlite3.connect('databases/database2.db')

# Create tables
CreateTable(conn1, conn2).create_tables()

# Check tables
CheckTables(conn1, conn2).check_all_tables()

# Perform the pipelined hash join
hash_join = HashJoin(conn1, conn2)
start_time = time.time()
hash_join.perform_pipelined_hash_join()
end_time = time.time()
running_time = end_time - start_time

logging.info(f"\nTotal matches (pipelined hash join): {hash_join.counter}\n")
logging.info(f"Running time (pipelined hash join): {running_time} seconds\n")

# Perform the semi-join
semi_join = SemiJoin(conn1, conn2)
start_time = time.time()
semi_join.perform_semi_join()
end_time = time.time()
running_time = end_time - start_time

logging.info(f"\nTotal matches (semi-join): {semi_join.counter}\n")
logging.info(f"Running time (semi-join): {running_time} seconds")

# Close the database connections
conn1.close()
conn2.close()
