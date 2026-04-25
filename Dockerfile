FROM python:3.11-slim

LABEL maintainer="uts-aggregator"
LABEL description="Pub-Sub Log Aggregator — idempotent consumer + SQLite dedup"

WORKDIR /app

RUN adduser --disabled-password --gecos '' appuser \
    && mkdir -p /app/data \
    && chown -R appuser:appuser /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY tests/ ./tests/
COPY pytest.ini ./

RUN chown -R appuser:appuser /app

USER appuser

VOLUME ["/app/data"]

EXPOSE 8080

ENV DEDUP_DB_PATH=/app/data/dedup.db
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

CMD ["python", "-m", "uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8080"]
