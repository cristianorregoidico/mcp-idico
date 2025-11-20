FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       default-jdk-headless \
       curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# JAVA_HOME usando el "default-java" de Debian
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Instalar uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:${PATH}"

WORKDIR /app

COPY pyproject.toml uv.lock* ./
RUN uv sync

COPY . .

EXPOSE 8000

CMD ["uv", "run", "main.py"]

