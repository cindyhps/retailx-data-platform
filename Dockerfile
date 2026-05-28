FROM apache/airflow:2.8.0

RUN pip install --no-cache-dir duckdb==0.10.0 confluent-kafka==2.3.0
