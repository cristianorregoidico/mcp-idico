FROM --platform=linux/amd64 python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       openjdk-17-jdk-headless \
       curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Instalar uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:${PATH}"

WORKDIR /app

# Copiar solo los archivos de dependencias primero
COPY pyproject.toml uv.lock* ./

# Resolver e instalar dependencias (crea .venv dentro del proyecto)
RUN uv sync

# Copiar el código de la app
COPY . .

EXPOSE 8000

# Lanzar la app usando el entorno que maneja uv
CMD ["uv", "run", "main.py"]
