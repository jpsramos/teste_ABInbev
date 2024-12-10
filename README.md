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

1. Dockerfile
Arquivo que define como a imagem Docker será construída.

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

2. docker-compose.yml
Arquivo para orquestrar múltiplos containers.




version: "3.8"

services:

  spark-master:

  
    image: bde2020/spark-master:3.5.0-hadoop3.2
    
    container_name: spark-master
    
    ports:
    
      - "7077:7077"
      
      - "8080:8080"
    
    environment:
    
      - INIT_DAEMON_STEP=setup_spark

    networks:

      - spark-network



  spark-worker:

  
    image: bde2020/spark-worker:3.5.0-hadoop3.2
    
    container_name: spark-worker
    
    environment:
    
      - SPARK_MASTER=spark://spark-master:7077
    
    depends_on:
   
      - spark-master
    
    networks:
    
      - spark-network



  data-processor:
  
    build:
    
      context: .
      
      dockerfile: Dockerfile
    
    container_name: data-processor
    
    environment:
    
      - SPARK_MASTER=spark://spark-master:7077
    depends_on:
    
      - spark-master
    
    networks:
    
      - spark-network


networks:

  spark-network:
  
    driver: bridge

3. .dockerignore

Arquivo para excluir arquivos desnecessários da imagem Docker.




# Ignorar arquivos e diretórios desnecessários

.venv

__pycache__

*.pyc

*.pyo

*.log

data/

4. requirements.txt

Arquivo com as dependências do projeto.




pyspark==3.5.0

pandas==1.5.3

pyarrow==12.0.1

5. DAG do Airflow

Arquivo Python para orquestrar o fluxo de trabalho no Airflow.




import sys


import os

import airflow

from airflow import DAG

from airflow.operators.python import PythonOperator

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Adicione o diretório base do projeto ao sys.path

base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

sys.path.append(base_path)

# Agora você pode importar a classe corretamente

from jobs.utils.api.restfull import DataProcessor as restfull

# Função Python que será executada no novo step

def run_restfull_api():

    print("Running custom Python script...")

    # Instancia a classe DataProcessor
    
    processor = restfull()
    
    # Chama o método `run` da classe para executar o processo
    
    output = processor.run(subdirectory="Breweries")
    
    print("Custom Python script executed successfully.")
    
    return output['layer']

# Configuração da DAG
dag = DAG(
  
    dag_id="teste",
    
    default_args={
    
        "owner": "Jonnathans Silva",
        
        "start_date": airflow.utils.dates.days_ago(1),
    },

    schedule_interval="@daily",
)

start = PythonOperator(
    task_id="start",
    python_callable=lambda: print("Jobs started"),
    dag=dag,
)

# Novo step que executa um programa Python
api_step = PythonOperator(
    task_id="api_step",
    python_callable=run_restfull_api,  # Chama a função definida acima
    dag=dag,
)

bronze_job = SparkSubmitOperator(
    task_id="bronze_job",
    conn_id="spark-conn",
    application="jobs/scripts/ingestion.py",
    conf={
        'spark.master': 'spark://spark-master:7077',  # Use o cluster Spark
        'layer': "{{ task_instance.xcom_pull(task_ids='api_step') }}",
    },
    dag=dag,
)

silver_job = SparkSubmitOperator(
    task_id="silver_job",
    conn_id="spark-conn",
    application="jobs/scripts/ingestion.py",
    conf={
        'spark.master': 'spark://spark-master:7077',  # Use o cluster Spark
        'layer': "silver",
    },
    dag=dag,
)

end = PythonOperator(
    task_id="end",
    python_callable=lambda: print("Jobs completed successfully"),
    dag=dag,
)

# Definindo a ordem de execução
start >> api_step >> bronze_job >> silver_job >> end
6. Passos para Construção e Execução
Construir a Imagem Docker:




docker build -t teste_abinbev .
Subir os Containers:




docker-compose up
Acessar o Container:




docker exec -it data-processor bash
Executar o Script de Ingestão:




python jobs/scripts/ingestion.py
