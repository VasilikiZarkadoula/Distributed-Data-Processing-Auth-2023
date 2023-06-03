import subprocess

scripts = [
    "create_table.py",
    "check_tables.py",
    "pipelined_hash_join.py",
    "semi_join.py"
]

for script in scripts:
    subprocess.run(["python", script])
