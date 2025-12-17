# Usa uma imagem base Python leve
FROM python:3.10-slim

# Define o diretório de trabalho dentro do contêiner
WORKDIR /app

# O 'requirements.txt' contendo: requests, minio, pandas, etc.
COPY configs/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o restante do código da aplicação
COPY . .

