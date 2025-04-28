# Use slim Python image
FROM python:3.12.4-slim

# Switch to root for dependency installation
USER root

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    POETRY_HOME=/opt/poetry \
    PATH="/opt/poetry/bin:$PATH" \
    AWS_REGION=eu-west-1 \
    AWS_DEFAULT_REGION=eu-west-1

# Install system dependencies in a single layer
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    libpq-dev \
    build-essential \
    python3-dev \
    wget \
    unzip \
    && rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*

# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python3 -

# Set working directory
WORKDIR /app

# Copy project files
COPY pyproject.toml poetry.lock* ./

# Install project dependencies
RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-root

# Create a non-root user
RUN useradd -m -u 1000 appuser \
    && chown -R appuser:appuser /app

# Copy the rest of the project files
COPY . /app

# Change ownership of the application directory
RUN chown -R appuser:appuser /app

# Install AWS CLI
RUN pip install awscli

# Optional: Install Vault
#RUN wget https://releases.hashicorp.com/vault/1.10.3/vault_1.10.3_linux_amd64.zip \
#    && unzip vault_1.10.3_linux_amd64.zip \
#    && mkdir -p /usr/local/bin \
#    && mv vault /usr/local/bin/ \
#    && rm vault_1.10.3_linux_amd64.zip

# Switch to non-root user
USER appuser
WORKDIR /app

# Default command (can be overridden)
CMD ["/bin/bash"]