FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    postgresql-client \
    libpq-dev \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN pip install --no-cache-dir poetry==1.7.0

# Copy dependency files
COPY pyproject.toml ./

# Configure poetry to not create virtual env (we're in a container)
RUN poetry config virtualenvs.create false

# Install dependencies (lock and install from pyproject.toml)
RUN poetry lock --no-update && poetry install --no-interaction --no-ansi --no-root

# Copy application code
COPY src/ ./src/
COPY tests/ ./tests/

# Install the package
RUN poetry install --no-interaction --no-ansi

CMD ["tail", "-f", "/dev/null"]