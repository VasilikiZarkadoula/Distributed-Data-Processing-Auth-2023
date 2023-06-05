# Use the official Python base image
FROM python:3.7.9

# Set the working directory in the container
WORKDIR /app

# Install the Python dependencies
RUN pip install --no-cache-dir --upgrade pip

# Copy the application code to the container
COPY . .

# Copy the databases folder into the container
COPY databases/ /app/databases/

# Define the default command to run when the container starts
CMD ["python", "main.py"]
