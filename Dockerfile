# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /usr/src/app

# Copy the current directory contents into the container at /usr/src/app
COPY . .

# If your project has dependencies in a requirements.txt file, uncomment the following line and ensure the file is in the project root
RUN pip install --no-cache-dir -r requirements.txt

# Set the PYTHONPATH environment variable to include the src directory
ENV PYTHONPATH "${PYTHONPATH}:/usr/src/app/src"

# Run module1.py when the container launches
CMD ["python", "./src/optimization/trading/trading_optimization_handler.py"]
