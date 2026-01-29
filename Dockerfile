# ---- builder ----
FROM python:3.12-slim AS builder

ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       default-jdk-headless \
       curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Instalar uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:${PATH}"

# Copiar manifests y resolver deps
COPY pyproject.toml uv.lock* ./
RUN uv sync

# Copiar el resto del código
COPY . .

# ---- runtime ----
FROM python:3.12-slim AS runtime

ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       default-jdk-headless \
       ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Copiar uv y el entorno/resolved deps desde builder
COPY --from=builder /root/.local /root/.local
ENV PATH="/root/.local/bin:${PATH}"

# Copiar app y lo instalado en /app (incluye .venv/si uv lo crea ahí)
COPY --from=builder /app /app

EXPOSE 8000
CMD ["uv", "run", "main.py"]
