# teste_ABInbev
Teste de engenharia realizado para aferir conhecimento
# Data Processing Project

## Descrição
Este projeto realiza o processamento de dados utilizando PySpark e Pandas. Ele é projetado para gerenciar diretórios, ler arquivos CSV ou Parquet e salvar os resultados processados em diferentes camadas de dados (`bronze` e `silver`).

## Requisitos
Certifique-se de que os seguintes requisitos estão instalados antes de executar o projeto:

- Python 3.8 ou superior
- PySpark 3.5.0
- Pandas 1.5.3
- PyArrow 12.0.1

## Estrutura do Projeto
Projeto ABInbev
├── .venv
├── data/
│   ├── transient/
│   │   └── Breweries/
│   │       └── YYYY-MM-DD/
│   │           └── *.csv
│   ├── bronze/
│   │   └── Breweries/
│   │       └── dat_process=YYYY-MM-DD/
│   │           └── *.parquet
│   └── silver/
│       └── Breweries/
│           └── dat_process=YYYY-MM-DD/
│               └── *.parquet
├── jobs
│   ├── scripts/
│   │   └── ingestion.py/
├── utils
│   ├── api/
│   │   └── restfull.py/
├── config/
│   ├── config.py
├── docker-compose.yml
├── Dockerfile
└── README.md└── README.md
├── airfloe.env
└── .dockerignore

# Projeto de Processamento de Dados com Docker, Spark e Airflow

Este projeto utiliza **Docker** para criar um ambiente isolado, **Spark** para processamento distribuído de dados e **Airflow** para orquestração de workflows.

---

## **1. Dockerfile**

O arquivo `Dockerfile` define como a imagem Docker será construída, configurando o ambiente necessário para rodar o projeto com Spark e Airflow.

### Conteúdo do Dockerfile:

```dockerfile
# Etapa 1: Escolha da imagem base
FROM python:3.8-slim

# Etapa 2: Configuração do ambiente
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Etapa 3: Instalação de dependências do sistema
RUN apt-get update && apt-get install -y \
    build-essential \
    default-jdk \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Etapa 4: Instalação do Spark
ENV SPARK_VERSION=3.5.0
RUN curl -O https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz -C /opt && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz
ENV SPARK_HOME=/opt/spark-${SPARK_VERSION}-bin-hadoop3
ENV PATH=$SPARK_HOME/bin:$PATH

# Etapa 5: Configuração do diretório de trabalho
WORKDIR /app

# Etapa 6: Copiar os arquivos do projeto para o container
COPY . /app

# Etapa 7: Instalação de dependências do Python
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Etapa 8: Exposição de portas (se necessário)
EXPOSE 8080

# Etapa 9: Comando de inicialização
CMD ["python", "jobs/scripts/ingestion.py"]
