FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY app /app/app
COPY worker /app/worker
COPY alembic /app/alembic
COPY alembic.ini /app/alembic.ini

ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "app.db_init"]
