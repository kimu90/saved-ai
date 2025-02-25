# Use Python 3.11 slim image
FROM python:3.11-slim

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN pip install --no-cache-dir poetry

# Set working directory
WORKDIR /app

# Copy only pyproject.toml and poetry.lock for dependency installation
COPY ai-services/pyproject.toml ai-services/poetry.lock* ./

# Install dependencies using Poetry
RUN poetry config virtualenvs.create false \
    && poetry install --no-root --no-interaction --no-ansi

# Install additional packages with pip
RUN pip install --index-url https://download.pytorch.org/whl/cpu torch && \
    pip install sentence-transformers && \
    pip install faiss-cpu==1.9.0.post1 && \
    pip install \
        apache-airflow==2.7.3 \
        apache-airflow-providers-celery==3.3.1 \
        apache-airflow-providers-postgres==5.6.0 \
        apache-airflow-providers-redis==3.3.1 \
        apache-airflow-providers-http==4.1.0 \
        apache-airflow-providers-common-sql==1.10.0 \
        croniter==2.0.1 \
        cryptography==42.0.0

# Copy application code
COPY . /app

# Creates a non-root user and assigns permissions
RUN adduser -u 5678 --disabled-password --gecos "" appuser && chown -R appuser /app
USER appuser

# Default command
CMD ["python", "ai-services/ai_services_api/main.py"]
