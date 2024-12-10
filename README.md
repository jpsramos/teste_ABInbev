# teste_ABInbev
Teste de engenharia realizado para aferir conhecimento
# Data Processing Project

## Descrição

Este projeto utiliza **Docker** para criar um ambiente isolado, **Spark** para processamento distribuído de dados e **Airflow** para orquestração de workflows. Abaixo está a configuração do arquivo `docker-compose.yml` utilizado para gerenciar os containers do projeto.

---

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

--

### Conteúdo do `docker-compose.yml`

```yaml
version: '4'

x-spark-common: &spark-common
  image: bitnami/spark:3.5.0-debian-11-r0
  volumes:
    - ./jobs:/opt/bitnami/spark/jobs
    - ./data:/opt/airflow/data
  networks:
    - code-with-yu

x-airflow-common: &airflow-common
  build:
    context: .
    dockerfile: Dockerfile
  env_file:
    - airflow.env
  volumes:
    - ./jobs:/opt/airflow/jobs
    - ./dags:/opt/airflow/dags
    - ./logs:/opt/airflow/logs
    - ./data:/opt/airflow/data
  depends_on:
    - postgres
  networks:
    - code-with-yu

services:
  spark-master:
    <<: *spark-common
    container_name: spark-master
    command: bin/spark-class org.apache.spark.deploy.master.Master
    environment:
      PYSPARK_PYTHON: /usr/bin/python3.9
      PYSPARK_DRIVER_PYTHON: /usr/bin/python3.9
    ports:
      - "9090:8080"
      - "7077:7077"

  spark-worker:
    <<: *spark-common
    container_name: spark-worker
    command: bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077
    depends_on:
      - spark-master
    environment:
      SPARK_MODE: worker
      SPARK_WORKER_CORES: 2
      SPARK_WORKER_MEMORY: 1g
      SPARK_MASTER_URL: spark://spark-master:7077
      PYSPARK_PYTHON: /usr/bin/python3.9
      PYSPARK_DRIVER_PYTHON: /usr/bin/python3.9

  postgres:
    image: postgres:14.0
    container_name: airflow-postgres
    environment:
      - POSTGRES_USER=airflow
      - POSTGRES_PASSWORD=airflow
      - POSTGRES_DB=airflow
    networks:
      - code-with-yu

  webserver:
    <<: *airflow-common
    container_name: airflow-webserver
    command: webserver
    ports:
      - "8080:8080"
    depends_on:
      - scheduler

  scheduler:
    <<: *airflow-common
    container_name: airflow-scheduler
    command: bash -c "airflow db migrate && airflow users create --username admin --firstname Jonnathans --lastname Silva --role Admin --email airscholar@gmail.com --password admin && airflow scheduler"

networks:
  code-with-yu:
