FROM apache/spark:3.5.3-python3
USER root
RUN pip install --no-cache-dir psycopg2-binary==2.9.9
WORKDIR /app
COPY *.py /app/
