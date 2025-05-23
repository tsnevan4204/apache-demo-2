# Dockerfile
FROM apache/airflow:3.0.1-python3.10-slim

# Copy your requirements file
COPY requirements.txt .

# Install Python packages
RUN pip install --no-cache-dir -r requirements.txt