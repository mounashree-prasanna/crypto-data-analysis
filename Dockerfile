FROM apache/airflow:2.10.1-python3.12

COPY requirements.txt .

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt --config-settings=resolver.max_rounds=500000