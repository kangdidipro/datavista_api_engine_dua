# Gunakan image Python 3.10 yang lebih stabil
FROM python:3.10-slim

# Set working directory di dalam container
WORKDIR /app

# Instal curl
RUN apt-get update && apt-get install -y curl postgresql-client

# Instal dependencies (FastAPI, Pandas, Psycopg2, Redis, Gunicorn)
RUN pip install fastapi uvicorn[standard] psycopg2-binary pandas python-multipart redis gunicorn rq sqlalchemy requests alembic
RUN pip install openpyxl
# Salin kode aplikasi (main.py, db_config.py, routers/, models/, dll.)
COPY . /app

# Perintah untuk menjalankan aplikasi (Gunicorn - Exec Form yang stabil)
CMD ["gunicorn", "main:app", "-w", "4", "-k", "uvicorn.workers.UvicornWorker", "-b", "0.0.0.0:8003"]
