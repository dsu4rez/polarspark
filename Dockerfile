FROM python:3.11-slim

# Install Java (required by pyspark) and common build tools
RUN apt-get update \
    && apt-get install --no-install-recommends --yes \
       ca-certificates \
       curl \
       gnupg \
       git \
       default-jre-headless \
       build-essential \
       libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Task (https://taskfile.dev/)
RUN sh -c "$(curl --location https://taskfile.dev/install.sh)" -- -b /usr/local/bin

# Write a JAVA_HOME profile so shells and processes see a correct value
RUN JAVA_HOME_PATH=$(dirname $(dirname $(readlink -f $(which java)))) \
    && echo "export JAVA_HOME=${JAVA_HOME_PATH}" > /etc/profile.d/java_home.sh \
    && chmod +x /etc/profile.d/java_home.sh

WORKDIR /workspace

# Upgrade pip and install Python packages used during development
RUN pip install --no-cache-dir --upgrade pip setuptools wheel
RUN pip install --no-cache-dir polars pandas pyspark pytest ruff

# Create a non-root user for safer development
RUN useradd --create-home devuser && chown -R devuser:devuser /workspace
USER devuser

ENV PYTHONPATH="/workspace"
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUTF8=1
