FROM svizor/zoomcamp-model:mlops-3.10.0-slim

WORKDIR /app
# Copy the requirements file to the working directory
COPY requirements.txt .

RUN pip install --upgrade pip
# Install project dependencies
RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p output
COPY week4_homework.py .