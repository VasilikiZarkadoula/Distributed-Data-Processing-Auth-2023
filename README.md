# Distributed Data Processing Auth2023 - Docker Application


## Prerequisites

To run this application, you need to have Docker installed on your machine. You can download and install Docker from the official website: [https://www.docker.com](https://www.docker.com)

## Getting Started

Follow the steps below to run the application using Docker:

1. Open a terminal and clone the repository to your local machine:

git clone https://github.com/your-username/your-repository.git


2. Navigate to the repository directory:

cd your-repository


3. Build the Docker image using the provided Dockerfile:

docker build -t myapp .

4. Run the Docker container:

docker run myapp


This will execute the Python scripts in the following order:
1. `create_table.py`: Creates tables in the databases.
2. `check_tables.py`: Performs checks on the tables.
3. `pipelined_hash_join.py`: Executes the pipelined hash join algorithm.
4. `semi_join.py`: Performs the semi-join operation.

Note: The databases `database1.db` and `database2.db` will be created within the Docker container.

5. Once the container finishes running, you can access the results or any generated output as per the functionality of the Python scripts.


